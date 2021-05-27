package dump

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"time"

	"github.com/busoc/mmaconv"
)

var SplitHeaders = []string{
	"time",
	"upi",
	"sequence",
	"vmu-sequence",
	"Tx [degC]",
	"Ty [degC]",
	"Tz [degC]",
	"Ax [microG]",
	"Ay [microG]",
	"Az [microG]",
}

const (
	timeFormat      = "2006.002.15.04.05.000000"
	isoFormat       = "2006-01-02T15:04:05.000000"
	splitFieldCount = 10
	flatFieldCount  = (3 * mmaconv.MeasCount) + 7
	allFieldDiff    = 9
)

type Flag struct {
	Iso  bool
	All  bool
	Time time.Duration
}

func Split(ws *csv.Writer, data []mmaconv.Measurement, freq float64, set Flag) (time.Time, error) {
	var now time.Time
	if len(data) == 0 {
		return now, nil
	}
	var (
		size = splitFieldCount
		tf   = timeFormat
	)
	if set.All {
		size += allFieldDiff
	}
	if set.Iso {
		tf = isoFormat
	}
	var (
		str     = make([]string, 0, size)
		delta   = time.Duration(freq*1_000_000_000) * time.Nanosecond
		prev    uint16
		elapsed time.Duration
	)
	if set.Time > 0 {
		delta = set.Time
	}
	for i, m := range data {
		curr := uint16(m.Seq)
		if d := sequenceDelta(i, curr, prev); elapsed > 0 && d > 0 && d != mmaconv.MeasCount {
			fmt.Println(prev, curr, d)
			elapsed += delta * time.Duration(d/mmaconv.MeasCount)
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			if m.NoDate {
				str = append(str, "")
			} else {
				now = m.When.Add(elapsed)
				str = append(str, now.Format(tf))
			}
			str = append(str, m.UPI)
			str = append(str, formatSequence(m.Seq))
			str = append(str, formatSequence2(m.Vid))
			str = append(str, formatFloat(m.DegX))
			str = append(str, formatFloat(m.DegY))
			str = append(str, formatFloat(m.DegZ))
			if set.All {
				str = appendFields(str, m)
			}
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
			if err := ws.Write(str); err != nil {
				return now, err
			}
			str = str[:0]
			if !m.NoDate {
				elapsed += delta
			}
		}
		prev = curr
	}
	ws.Flush()
	return now, ws.Error()
}

func Flat(ws *csv.Writer, data []mmaconv.Measurement, freq float64, set Flag) (time.Time, error) {
	var now time.Time
	if len(data) == 0 {
		return now, nil
	}
	var (
		size = flatFieldCount
		tf   = timeFormat
	)
	if set.All {
		size += allFieldDiff
	}
	if set.Iso {
		tf = isoFormat
	}
	var (
		str     = make([]string, 0, size)
		delta   = time.Duration(freq*1_000_000) * time.Microsecond
		prev    uint16
		elapsed time.Duration
	)
	if set.Time > 0 {
		delta = set.Time
	}
	for i, m := range data {
		curr := uint16(m.Seq)
		if d := sequenceDelta(i, curr, prev); elapsed > 0 && d > 0 && d != mmaconv.MeasCount {
			elapsed += delta * time.Duration(d/mmaconv.MeasCount)
		}
		if m.NoDate {
			str = append(str, "")
		} else {
			now = m.When.Add(elapsed)
			str = append(str, now.Format(tf))
		}
		str = append(str, m.UPI)
		str = append(str, formatSequence(m.Seq))
		str = append(str, formatSequence2(m.Vid))
		str = append(str, formatFloat(m.DegX))
		str = append(str, formatFloat(m.DegY))
		str = append(str, formatFloat(m.DegZ))
		if set.All {
			str = appendFields(str, m)
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
		}
		if err := ws.Write(str); err != nil {
			return now, err
		}
		if !m.NoDate {
			elapsed += mmaconv.MeasCount * delta
		}
		prev = curr
		str = str[:0]
	}
	ws.Flush()
	return now, ws.Error()
}

func appendFields(str []string, m mmaconv.Measurement) []string {
	str = append(str, formatFloat(m.MicX))
	str = append(str, formatFloat(m.MicY))
	str = append(str, formatFloat(m.MicZ))
	str = append(str, formatFloat(m.ScaleX))
	str = append(str, formatFloat(m.OffsetX))
	str = append(str, formatFloat(m.ScaleY))
	str = append(str, formatFloat(m.OffsetY))
	str = append(str, formatFloat(m.ScaleZ))
	str = append(str, formatFloat(m.OffsetZ))
	return str
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatSequence2(v uint32) string {
	return strconv.FormatUint(uint64(v), 10)
}

func formatSequence(v uint16) string {
	return strconv.FormatUint(uint64(v), 10)
}

func sequenceDelta(iter int, curr, prev uint16) uint16 {
	if iter <= 0 {
		return 0
	}
	if curr < prev {
		return curr + (mmaconv.MaxSequence - prev) + 1
	}
	return curr - prev
}
