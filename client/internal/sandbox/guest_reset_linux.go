package sandbox

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

const guestNIC = "eth0"

// applyReset 在从快照恢复出来的虚机里重设时钟、随机数种子和网卡地址。
func applyReset(r *guestReset) error {
	if r.UnixNano != 0 {
		ts := unix.NsecToTimespec(r.UnixNano)
		if err := unix.ClockSettime(unix.CLOCK_REALTIME, &ts); err != nil {
			return fmt.Errorf("设时钟: %w", err)
		}
	}
	if len(r.Seed) > 0 {
		if err := reseed(r.Seed); err != nil {
			return fmt.Errorf("重设随机数种子: %w", err)
		}
	}
	if r.IP == "" {
		return nil
	}
	ip, gw := net.ParseIP(r.IP).To4(), net.ParseIP(r.Gateway).To4()
	if ip == nil || gw == nil {
		return errors.New("网卡地址或网关不是 IPv4 地址")
	}
	return setAddress(ip, gw)
}

// reseed 把 seed 混进内核的熵池并让内核立刻重新播种,否则同一快照恢复出的虚机会给出同一串随机数。
func reseed(seed []byte) error {
	f, err := os.OpenFile("/dev/urandom", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(seed); err != nil {
		return err
	}
	return unix.IoctlSetInt(int(f.Fd()), unix.RNDRESEEDCRNG, 0) //nolint:gosec // fd 不会溢出 int
}

// setAddress 把网卡地址换成 ip/24,默认路由指向 gw。换地址时内核会清掉这块网卡上的路由,所以路由要重加。
func setAddress(ip, gw net.IP) error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	for _, set := range []struct {
		req  uint
		addr []byte
	}{{unix.SIOCSIFADDR, ip}, {unix.SIOCSIFNETMASK, net.IPv4(255, 255, 255, 0).To4()}} {
		ifr, ifrErr := unix.NewIfreq(guestNIC)
		if ifrErr != nil {
			return ifrErr
		}
		if stepErr := ifr.SetInet4Addr(set.addr); stepErr != nil {
			return stepErr
		}
		if err = unix.IoctlIfreq(fd, set.req, ifr); err != nil {
			return fmt.Errorf("设网卡地址: %w", err)
		}
	}
	nic, err := net.InterfaceByName(guestNIC)
	if err != nil {
		return err
	}
	return addDefaultRoute(gw, uint32(nic.Index)) //nolint:gosec // 网卡编号是小的正数
}

// addDefaultRoute 经 netlink 加一条 default via gw。虚机镜像里不一定有 ip 命令,所以自己发这一条消息。
func addDefaultRoute(gw net.IP, ifindex uint32) error {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	le := binary.LittleEndian // 虚机只有 amd64 和 arm64,都是小端
	const msgLen = unix.SizeofNlMsghdr + unix.SizeofRtMsg + 8 + 8
	msg := make([]byte, 0, msgLen)
	msg = le.AppendUint32(msg, msgLen)
	msg = le.AppendUint16(msg, unix.RTM_NEWROUTE)
	msg = le.AppendUint16(msg, unix.NLM_F_REQUEST|unix.NLM_F_ACK|unix.NLM_F_CREATE|unix.NLM_F_REPLACE)
	msg = le.AppendUint32(msg, 1) // 序号
	msg = le.AppendUint32(msg, 0) // 发送方端口,内核自己填
	// rtmsg:IPv4、目的前缀长度 0(默认路由)、主路由表、静态路由、全局范围、单播。
	msg = append(msg, unix.AF_INET, 0, 0, 0, unix.RT_TABLE_MAIN, unix.RTPROT_BOOT, unix.RT_SCOPE_UNIVERSE, unix.RTN_UNICAST)
	msg = le.AppendUint32(msg, 0) // rtm_flags
	msg = le.AppendUint16(msg, 8)
	msg = le.AppendUint16(msg, unix.RTA_GATEWAY)
	msg = append(msg, gw...)
	msg = le.AppendUint16(msg, 8)
	msg = le.AppendUint16(msg, unix.RTA_OIF)
	msg = le.AppendUint32(msg, ifindex)
	if stepErr := unix.Sendto(fd, msg, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); stepErr != nil {
		return stepErr
	}
	reply := make([]byte, 4096)
	n, _, err := unix.Recvfrom(fd, reply, 0)
	if err != nil {
		return err
	}
	// 应答是 nlmsghdr 后跟一个 int32 错误码,0 表示成功,负数是 -errno。
	if n < unix.SizeofNlMsghdr+4 || le.Uint16(reply[4:]) != unix.NLMSG_ERROR {
		return errors.New("netlink 应答的格式不认识")
	}
	if code := int32(le.Uint32(reply[unix.SizeofNlMsghdr:])); code != 0 { //nolint:gosec // 内核给的就是 int32
		return fmt.Errorf("加默认路由: %w", unix.Errno(-code))
	}
	return nil
}
