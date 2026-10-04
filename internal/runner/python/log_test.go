package python

import (
	"fmt"
	"strings"
	"testing"
)

func TestLiveLogRedactsAcrossEveryWriteBoundary(t *testing.T) {
	values := map[string]string{"token": "private-token", "overlap": "private", "encoded": "hello world&", "multiline": "first\nsecond"}
	input := "東京 private-token hello+world%26 first\nsecond done\n"
	want := "東京 [REDACTED] [REDACTED] [REDACTED] done\n"
	for size := 1; size <= len(input); size++ {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var output strings.Builder
			log := newRedactedLog(values, func(text string) { output.WriteString(text) })
			for start := 0; start < len(input); start += size {
				log.Write([]byte(input[start:min(start+size, len(input))]))
				if !strings.HasPrefix(want, output.String()) {
					t.Fatalf("unsafe live output: %q", output.String())
				}
			}
			if output.Len() == 0 {
				t.Fatal("no output before completion")
			}
			log.flush(true)
			if output.String() != want {
				t.Fatalf("output = %q", output.String())
			}
		})
	}
}

func TestLiveLogPreservesSplitUnicodeWithoutSecrets(t *testing.T) {
	var output strings.Builder
	log := newRedactedLog(nil, func(text string) { output.WriteString(text) })
	for _, b := range []byte("東京\n") {
		log.Write([]byte{b})
	}
	log.flush(true)
	if output.String() != "東京\n" {
		t.Fatalf("output = %q", output.String())
	}
}
