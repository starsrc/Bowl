package config_test

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseDataDirFromCompose 从 docker-compose.yaml.example 的 storage-service 环境变量定义中解析持久化数据目录。
// 优先采用 DATA_DIR，次优先采用 STORAGE_DATA_DIR。
func parseDataDirFromCompose(content string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(content))
	inStorageService := false
	inEnv := false
	var dataDir, storageDataDir string

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			continue
		}

		// 识别 storage-service 顶层定义
		if strings.HasPrefix(line, "  storage-service:") {
			inStorageService = true
			inEnv = false
			continue
		} else if inStorageService && len(line) > 0 && line[0] != ' ' {
			// 返回至顶层
			break
		} else if inStorageService && strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "  storage-service:") {
			// 进入其他平级服务
			break
		}

		if inStorageService {
			if strings.HasPrefix(line, "    environment:") {
				inEnv = true
				continue
			} else if inEnv && !strings.HasPrefix(line, "      ") {
				inEnv = false
			}

			if inEnv {
				parts := strings.SplitN(trimmed, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					val := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
					if key == "DATA_DIR" {
						dataDir = val
					} else if key == "STORAGE_DATA_DIR" {
						storageDataDir = val
					}
				}
			}
		}
	}

	if dataDir != "" {
		return dataDir, nil
	}
	if storageDataDir != "" {
		return storageDataDir, nil
	}
	return "", fmt.Errorf("neither DATA_DIR nor STORAGE_DATA_DIR found in storage-service environment")
}

// parseDockerfileRuntimeInstructions 解析 Dockerfile 最终运行阶段（最后一个 FROM 阶段）的逻辑指令序列，
// 正确拼接行末 "\" 续行符并忽略注释行与空行。
func parseDockerfileRuntimeInstructions(dockerfileContent string) ([]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(dockerfileContent))
	var allInstructions []string
	var current strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasSuffix(line, "\\") {
			trimmed := strings.TrimSuffix(line, "\\")
			current.WriteString(strings.TrimSpace(trimmed))
			current.WriteString(" ")
		} else {
			current.WriteString(line)
			allInstructions = append(allInstructions, strings.TrimSpace(current.String()))
			current.Reset()
		}
	}
	if current.Len() > 0 {
		allInstructions = append(allInstructions, strings.TrimSpace(current.String()))
	}

	lastFromIndex := -1
	for i, instr := range allInstructions {
		if strings.HasPrefix(strings.ToUpper(instr), "FROM ") {
			lastFromIndex = i
		}
	}

	if lastFromIndex == -1 {
		return nil, errors.New("no FROM instruction found in Dockerfile")
	}

	return allInstructions[lastFromIndex:], nil
}

// instructionHasMkdir 检查 RUN 指令是否预建指定数据目录。
func instructionHasMkdir(instr string, dataDir string) bool {
	if !strings.Contains(instr, "mkdir") {
		return false
	}
	return strings.Contains(instr, dataDir)
}

// instructionHasChown 检查 RUN 指令是否将数据目录（或其父目录）递归/显式所有权赋予非 root 用户。
func instructionHasChown(instr string, dataDir string, user string) bool {
	if !strings.Contains(instr, "chown") {
		return false
	}

	hasUser := strings.Contains(instr, user) ||
		(user == "appuser" && strings.Contains(instr, "10001")) ||
		(user == "10001" && strings.Contains(instr, "appuser"))
	if !hasUser {
		return false
	}

	if strings.Contains(instr, dataDir) {
		return true
	}

	cleanDataDir := filepath.Clean(dataDir)
	parentDir := filepath.Dir(cleanDataDir)
	for parentDir != "" && parentDir != "/" && parentDir != "." {
		if strings.Contains(instr, parentDir) && strings.Contains(instr, "-R") {
			return true
		}
		parentDir = filepath.Dir(parentDir)
	}

	return false
}

// validateDockerfileNonRootDataDir 校验 Dockerfile 运行阶段是否在切换非 root 用户前预建并正确赋权持久化数据目录。
func validateDockerfileNonRootDataDir(dockerfileContent string, dataDir string) error {
	if dataDir == "" {
		return errors.New("dataDir must not be empty")
	}

	instructions, err := parseDockerfileRuntimeInstructions(dockerfileContent)
	if err != nil {
		return err
	}

	userCreatedIdx := -1
	nonRootUser := ""
	for i, instr := range instructions {
		upper := strings.ToUpper(instr)
		if strings.HasPrefix(upper, "RUN ") && (strings.Contains(instr, "adduser") || strings.Contains(instr, "useradd")) {
			if strings.Contains(instr, "appuser") {
				userCreatedIdx = i
				nonRootUser = "appuser"
				break
			}
			if strings.Contains(instr, "10001") {
				userCreatedIdx = i
				nonRootUser = "10001"
				break
			}
		}
	}
	if userCreatedIdx == -1 {
		return errors.New("no non-root user creation (appuser / 10001) found in runtime stage")
	}

	userSwitchIdx := -1
	for i, instr := range instructions {
		upper := strings.ToUpper(instr)
		if strings.HasPrefix(upper, "USER ") {
			target := strings.TrimSpace(instr[5:])
			parts := strings.Split(target, ":")
			u := strings.TrimSpace(parts[0])
			if u != "root" && u != "0" {
				userSwitchIdx = i
				break
			}
		}
	}
	if userSwitchIdx == -1 {
		return errors.New("no non-root USER switch instruction found in runtime stage")
	}

	if userCreatedIdx >= userSwitchIdx {
		return errors.New("non-root USER switch occurs before user creation")
	}

	hasMkdirBeforeUser := false
	hasChownBeforeUser := false
	for i := userCreatedIdx; i < userSwitchIdx; i++ {
		instr := instructions[i]
		if strings.HasPrefix(strings.ToUpper(instr), "RUN ") {
			if instructionHasMkdir(instr, dataDir) {
				hasMkdirBeforeUser = true
			}
			if instructionHasChown(instr, dataDir, nonRootUser) {
				hasChownBeforeUser = true
			}
		}
	}

	hasMkdirAfterUser := false
	hasChownAfterUser := false
	for i := userSwitchIdx; i < len(instructions); i++ {
		instr := instructions[i]
		if strings.HasPrefix(strings.ToUpper(instr), "RUN ") {
			if instructionHasMkdir(instr, dataDir) {
				hasMkdirAfterUser = true
			}
			if instructionHasChown(instr, dataDir, nonRootUser) {
				hasChownAfterUser = true
			}
		}
	}

	if hasMkdirAfterUser || hasChownAfterUser {
		return fmt.Errorf("data directory mkdir/chown must occur before switching to non-root USER %q", nonRootUser)
	}

	if !hasMkdirBeforeUser && !hasChownBeforeUser {
		return fmt.Errorf("missing RUN instructions to mkdir -p %s and chown to non-root user %q before USER switch", dataDir, nonRootUser)
	}
	if !hasMkdirBeforeUser {
		return fmt.Errorf("missing RUN instruction to mkdir -p %s before USER switch", dataDir)
	}
	if !hasChownBeforeUser {
		return fmt.Errorf("missing RUN instruction to chown %s to non-root user %q before USER switch", dataDir, nonRootUser)
	}

	return nil
}

func TestStorageDockerfile_PreCreatesWritableDataDirBeforeNonRootUser(t *testing.T) {
	const expectedDataDir = "/var/lib/bowl/data"

	t.Run("ValidatorDiscrimination", func(t *testing.T) {
		validParentChown := `
FROM alpine:3.24 AS runtime
RUN apk --no-cache add ca-certificates tzdata
RUN addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup
RUN mkdir -p /var/lib/bowl/data && \
    chown -R appuser:appgroup /var/lib/bowl
WORKDIR /app
COPY --from=builder /bin/storage-service /app/storage-service
USER appuser:appgroup
EXPOSE 9000
ENTRYPOINT ["/app/storage-service"]
`
		assert.NoError(t, validateDockerfileNonRootDataDir(validParentChown, expectedDataDir), "valid Dockerfile with parent recursive chown should pass")

		validDirectChown := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
RUN mkdir -p /var/lib/bowl/data && chown -R appuser /var/lib/bowl/data
USER appuser
ENTRYPOINT ["/app/storage-service"]
`
		assert.NoError(t, validateDockerfileNonRootDataDir(validDirectChown, expectedDataDir), "valid Dockerfile with direct chown should pass")

		missingMkdirAndChown := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
USER appuser
ENTRYPOINT ["/app/storage-service"]
`
		err := validateDockerfileNonRootDataDir(missingMkdirAndChown, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing RUN instructions")

		mkdirAfterUser := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
USER appuser
RUN mkdir -p /var/lib/bowl/data && chown -R appuser /var/lib/bowl/data
ENTRYPOINT ["/app/storage-service"]
`
		err = validateDockerfileNonRootDataDir(mkdirAfterUser, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must occur before switching to non-root USER")

		missingChownOnly := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
RUN mkdir -p /var/lib/bowl/data
USER appuser
ENTRYPOINT ["/app/storage-service"]
`
		err = validateDockerfileNonRootDataDir(missingChownOnly, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing RUN instruction to chown")

		chownToRootOnly := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
RUN mkdir -p /var/lib/bowl/data && chown -R root:root /var/lib/bowl/data
USER appuser
ENTRYPOINT ["/app/storage-service"]
`
		err = validateDockerfileNonRootDataDir(chownToRootOnly, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing RUN instruction to chown")

		missingUserSwitch := `
FROM alpine:3.24 AS runtime
RUN adduser -u 10001 -S appuser
RUN mkdir -p /var/lib/bowl/data && chown -R appuser /var/lib/bowl/data
ENTRYPOINT ["/app/storage-service"]
`
		err = validateDockerfileNonRootDataDir(missingUserSwitch, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no non-root USER switch instruction found")

		missingUserCreation := `
FROM alpine:3.24 AS runtime
RUN mkdir -p /var/lib/bowl/data
USER appuser
ENTRYPOINT ["/app/storage-service"]
`
		err = validateDockerfileNonRootDataDir(missingUserCreation, expectedDataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no non-root user creation")
	})

	t.Run("RealDockerfileValidation", func(t *testing.T) {
		dockerfilePath := filepath.Join("..", "..", "Dockerfile")
		composePath := filepath.Join("..", "..", "..", "..", "docker-compose.yaml.example")
		if _, err := os.Stat(composePath); err != nil {
			if alt := filepath.Join("..", "..", "..", "docker-compose.yaml.example"); err == nil {
				composePath = alt
			}
		}

		dockerfileBytes, err := os.ReadFile(dockerfilePath)
		require.NoError(t, err, "failed to read services/storage/Dockerfile at path %s", dockerfilePath)

		composeBytes, err := os.ReadFile(composePath)
		require.NoError(t, err, "failed to read docker-compose.yaml.example at path %s", composePath)

		parsedDataDir, err := parseDataDirFromCompose(string(composeBytes))
		require.NoError(t, err, "failed to parse data directory from compose file")
		require.Equal(t, expectedDataDir, parsedDataDir)

		// 真实 services/storage/Dockerfile 目前未在 USER 切换前创建并赋予数据目录权限，
		// 在当前未修复代码上必须产生确定性真红 FAIL！
		err = validateDockerfileNonRootDataDir(string(dockerfileBytes), parsedDataDir)
		require.NoError(t, err, "services/storage/Dockerfile must pre-create and chown dataDir before USER switch")
	})
}
