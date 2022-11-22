package methods

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTopicFilterMatches(t *testing.T) {
	transferSym := xdr.ScSymbol("transfer")
	transfer := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &transferSym,
	}
	sixtyfour := xdr.Int64(64)
	number := xdr.ScVal{
		Type: xdr.ScValTypeScvU63,
		U63:  &sixtyfour,
	}
	star := "*"
	for _, tc := range []struct {
		name     string
		filter   TopicFilter
		includes []xdr.ScVec
		excludes []xdr.ScVec
	}{
		{
			name:   "<empty>",
			filter: nil,
			includes: []xdr.ScVec{
				{},
			},
			excludes: []xdr.ScVec{
				{transfer},
			},
		},

		// Exact matching
		{
			name: "ScSymbol(transfer)",
			filter: []SegmentFilter{
				{scval: &transfer},
			},
			includes: []xdr.ScVec{
				{transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{transfer, transfer},
			},
		},

		// Star
		{
			name: "*",
			filter: []SegmentFilter{
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer},
			},
			excludes: []xdr.ScVec{
				{transfer, transfer},
			},
		},
		{
			name: "*/transfer",
			filter: []SegmentFilter{
				{wildcard: &star},
				{scval: &transfer},
			},
			includes: []xdr.ScVec{
				{number, transfer},
				{transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer, number},
				{transfer},
				{transfer, number},
				{transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer, number},
				{transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer, number},
				{transfer},
				{number, transfer},
				{transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*/*",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer, number, number},
				{transfer, transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer},
				{number, transfer, number, number},
				{transfer},
				{transfer, transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*/number",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
				{scval: &number},
			},
			includes: []xdr.ScVec{
				{transfer, number, number},
				{transfer, transfer, number},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, number, number},
				{number, transfer, number},
				{transfer},
				{number, transfer},
				{transfer, transfer, transfer},
				{transfer, number, transfer},
			},
		},
	} {
		name := tc.name
		if name == "" {
			name = topicFilterToString(tc.filter)
		}
		t.Run(name, func(t *testing.T) {
			for _, include := range tc.includes {
				assert.True(
					t,
					tc.filter.Matches(include),
					"Expected %v filter to include %v",
					name,
					include,
				)
			}
			for _, exclude := range tc.excludes {
				assert.False(
					t,
					tc.filter.Matches(exclude),
					"Expected %v filter to exclude %v",
					name,
					exclude,
				)
			}

		})
	}
}

func TestTopicFilterJSON(t *testing.T) {
	var got TopicFilter

	assert.NoError(t, json.Unmarshal([]byte("[]"), &got))
	assert.Equal(t, TopicFilter{}, got)

	star := "*"
	assert.NoError(t, json.Unmarshal([]byte("[\"*\"]"), &got))
	assert.Equal(t, TopicFilter{{wildcard: &star}}, got)

	sixtyfour := xdr.Int64(64)
	scval := xdr.ScVal{Type: xdr.ScValTypeScvU63, U63: &sixtyfour}
	scvalstr, err := xdr.MarshalBase64(scval)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal([]byte(fmt.Sprintf("[%q]", scvalstr)), &got))
	assert.Equal(t, TopicFilter{{scval: &scval}}, got)
}

func topicFilterToString(t TopicFilter) string {
	var s []string
	for _, segment := range t {
		if segment.wildcard != nil {
			s = append(s, *segment.wildcard)
		} else if segment.scval != nil {
			out, err := xdr.MarshalBase64(*segment.scval)
			if err != nil {
				panic(err)
			}
			s = append(s, out)
		} else {
			panic("Invalid topic filter")
		}
	}
	if len(s) == 0 {
		s = append(s, "<empty>")
	}
	return strings.Join(s, "/")
}
