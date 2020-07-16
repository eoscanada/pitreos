package pitreos

import (
	"fmt"
	"regexp"
)

type Filter interface {
	Match(relativePath string) bool
}

type FilterFunc func(relativePath string) bool

func (f FilterFunc) Match(relativePath string) bool {
	return f(relativePath)
}

var AllFileFilter = FilterFunc(func(relativePath string) bool { return true })

type IncludeThanExcludeFilter struct {
	includeFilter *regexp.Regexp
	excludeFilter *regexp.Regexp
}

func NewIncludeThanExcludeFilter(includeFilter, excludeFilter string) (*IncludeThanExcludeFilter, error) {
	includeFilterRegex, err := filterStringPatternToRegexp(includeFilter)
	if err != nil {
		return nil, err
	}

	excludeFilterRegex, err := filterStringPatternToRegexp(excludeFilter)
	if err != nil {
		return nil, err
	}

	return &IncludeThanExcludeFilter{
		includeFilter: includeFilterRegex,
		excludeFilter: excludeFilterRegex,
	}, nil
}

func MustNewIncludeThanExcludeFilter(includeFilter, excludeFilter string) *IncludeThanExcludeFilter {
	filter, err := NewIncludeThanExcludeFilter(includeFilter, excludeFilter)
	if err != nil {
		panic(fmt.Errorf("unable to create include than exclude filter: %s", err))
	}

	return filter
}

func filterStringPatternToRegexp(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return regex, nil
}

func (f *IncludeThanExcludeFilter) Match(relativePath string) bool {
	if f.includeFilter != nil && !f.includeFilter.MatchString(relativePath) {
		return false
	}

	if f.excludeFilter != nil && f.excludeFilter.MatchString(relativePath) {
		return false
	}

	return true
}

func (f *IncludeThanExcludeFilter) String() string {
	if f == nil || (f.includeFilter != nil && f.excludeFilter == nil) {
		return "<No filtering>"
	}

	include := "'All'"
	if f.includeFilter != nil {
		include = f.includeFilter.String()
	}

	exclude := "'Nothing'"
	if f.includeFilter != nil {
		include = f.includeFilter.String()
	}

	return fmt.Sprintf("[Included %s, Excluded %s]", include, exclude)
}
