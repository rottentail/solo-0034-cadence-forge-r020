package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Loudness struct {
	IntegratedLUFS *float64 `json:"integrated_lufs"`
	TruePeakDBFS   *float64 `json:"true_peak_dbfs"`
	RangeLU        float64  `json:"range_lu"`
	Silent         bool     `json:"silent"`
}

func finite(s string) *float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || strings.Contains(strings.ToLower(s), "inf") || strings.Contains(strings.ToLower(s), "nan") {
		return nil
	}
	return &v
}

// Measure runs the independent loudness inspection scan. It owns its 40s
// deadline and its log-bound policy; process start/wait/exit reuse the shared
// media lifecycle instead of driving exec directly.
func Measure(ctx context.Context, binary, path string) (Loudness, error) {
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	args := []string{"-nostdin", "-hide_banner", "-protocol_whitelist", "file,pipe", "-format_whitelist", Formats, "-i", path, "-vn", "-af", "loudnorm=I=-16:TP=-1.5:LRA=11:print_format=json", "-f", "null", "-"}
	var log LimitedBuffer
	log.Limit = 32768
	proc, err := Start(ctx, binary, args, nil, &log)
	if err != nil {
		return Loudness{}, err
	}
	if err = proc.Wait(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return Loudness{}, err
		}
		return Loudness{}, fmt.Errorf("loudness scan: %w: %s", err, log.String())
	}
	text := log.String()
	start := strings.LastIndex(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end < start {
		return Loudness{}, fmt.Errorf("loudness output missing")
	}
	var raw struct {
		I   string `json:"input_i"`
		TP  string `json:"input_tp"`
		LRA string `json:"input_lra"`
	}
	if err := json.Unmarshal([]byte(text[start:end+1]), &raw); err != nil {
		return Loudness{}, err
	}
	l := Loudness{IntegratedLUFS: finite(raw.I), TruePeakDBFS: finite(raw.TP)}
	l.Silent = l.IntegratedLUFS == nil
	if v := finite(raw.LRA); v != nil {
		l.RangeLU = *v
	}
	return l, nil
}
