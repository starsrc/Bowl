package validator

import (
	"slices"

	"buf.build/go/protovalidate"
)

// ClassifiedFieldFullNames 返回分类器已注册的全部字段全名列表，供契约测试使用。
func ClassifiedFieldFullNames() []string {
	names := make([]string, 0, len(fieldClassifications))
	for name := range fieldClassifications {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// FieldRuleHasMaxLenBranch 返回字段在分类器中是否包含 maxLen 分支，供契约测试使用。
func FieldRuleHasMaxLenBranch(fullName string) (hasRule, hasMaxLen bool) {
	rule, ok := fieldClassifications[fullName]
	return ok, rule.hasMaxLen
}

// ClassifiedMessageRuleIDs 返回分类器已注册的全部消息级 CEL 规则 ID 列表，供契约测试使用。
func ClassifiedMessageRuleIDs() []string {
	ids := make([]string, 0, len(messageRulePriorities))
	for id := range messageRulePriorities {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// RegisteredMessageFullNames 返回 storageMessages 预编译列表中全部消息的全名，供契约测试使用。
func RegisteredMessageFullNames() []string {
	names := make([]string, 0, len(storageMessages))
	for _, msg := range storageMessages {
		names = append(names, string(msg.ProtoReflect().Descriptor().FullName()))
	}
	slices.Sort(names)
	return names
}

// ViolationFieldFullName 导出 violationFieldFullName 供单元测试使用。
func ViolationFieldFullName(v *protovalidate.Violation) string {
	return violationFieldFullName(v)
}

// ClassifyViolation 导出 classifyViolation 供优先级回归测试使用。
func ClassifyViolation(v *protovalidate.Violation) (priority int, reason, domain string) {
	return classifyViolation(v)
}
