package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var allowedGoDirectives = map[string]struct{}{
	"build":              {},
	"cgo_dynamic_linker": {},
	"cgo_export_dynamic": {},
	"cgo_export_static":  {},
	"cgo_import_dynamic": {},
	"cgo_import_static":  {},
	"cgo_ldflag":         {},
	"cgo_unsafe_args":    {},
	"debug":              {},
	"embed":              {},
	"fix":                {},
	"generate":           {},
	"linkname":           {},
	"noescape":           {},
	"noinline":           {},
	"norace":             {},
	"nosplit":            {},
	"notinheap":          {},
	"uintptrescapes":     {},
	"wasmexport":         {},
	"wasmimport":         {},
}

type violation struct {
	path   string
	line   int
	column int
}

func (v violation) String() string {
	return fmt.Sprintf("%s:%d:%d: comments are not allowed in Go source files", v.path, v.line, v.column)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		args = []string{"."}
	}

	files, err := collectGoFiles(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "nocomments: %v\n", err)
		return 2
	}

	var violations []violation
	for _, path := range files {
		fileViolations, checkErr := checkFile(path)
		if checkErr != nil {
			_, _ = fmt.Fprintf(stderr, "nocomments: %v\n", checkErr)
			return 2
		}
		violations = append(violations, fileViolations...)
	}

	for _, v := range violations {
		_, _ = fmt.Fprintln(stdout, v.String())
	}

	if len(violations) > 0 {
		return 1
	}
	return 0
}

func collectGoFiles(targets []string) ([]string, error) {
	seen := make(map[string]struct{})
	var files []string

	for _, target := range targets {
		cleanTarget := filepath.Clean(target)
		info, err := os.Stat(cleanTarget)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", target, err)
		}

		if !info.IsDir() {
			if strings.HasSuffix(cleanTarget, ".go") {
				if _, ok := seen[cleanTarget]; !ok {
					seen[cleanTarget] = struct{}{}
					files = append(files, cleanTarget)
				}
			}
			continue
		}

		walkErr := filepath.WalkDir(cleanTarget, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if path != cleanTarget && shouldSkipDir(d.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			if _, ok := seen[path]; !ok {
				seen[path] = struct{}{}
				files = append(files, path)
			}
			return nil
		})
		if walkErr != nil {
			return nil, fmt.Errorf("walk %s: %w", target, walkErr)
		}
	}

	sort.Strings(files)
	return files, nil
}

func shouldSkipDir(name string) bool {
	if strings.HasPrefix(name, ".") {
		return true
	}
	switch name {
	case "vendor", "gen", "node_modules":
		return true
	default:
		return false
	}
}

func checkFile(path string) ([]violation, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return checkSource(path, src)
}

func checkSource(path string, src []byte) ([]violation, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if ast.IsGenerated(file) {
		return nil, nil
	}

	var violations []violation
	for _, group := range file.Comments {
		for _, comment := range group.List {
			pos := fset.Position(comment.Slash)
			if isAllowedComment(comment.Text, pos.Column) {
				continue
			}
			violations = append(violations, violation{
				path:   path,
				line:   pos.Line,
				column: pos.Column,
			})
		}
	}
	return violations, nil
}

func isAllowedComment(text string, column int) bool {
	if column != 1 || !strings.HasPrefix(text, "//go:") {
		return false
	}
	rest := text[len("//go:"):]
	name, _, _ := strings.Cut(rest, " ")
	name, _, _ = strings.Cut(name, "\t")
	_, ok := allowedGoDirectives[name]
	return ok
}
