package mmaconv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/midbel/toml"
)

const (
	TempZero  = -273
	TempMMA   = 20
	TempDelta = TempZero - TempMMA
	MeasCount = 9
)

const (
	MaxSequence = (1 << 16) - 1
	MaxValue    = 1 << 15
)

var (
	Magic = []byte("MMA ")
	Epoch = time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)
)

var Frequencies = map[int64]int64{
	1500: 9,
	500:  27,
	300:  45,
	150:  90,
	50:   270,
	5:    2700,
}

var DefaultTable = Table{
	Frequency: 1500,
	Calib: XYZ{
		X: 293,
		Y: 293,
		Z: 293,
	},
	Scale: XYZ{
		X: 3.452,
		Y: 3.432,
		Z: 3.432,
	},
	Offset: XYZ{
		X: -1407.7,
		Y: -744.7,
		Z: -214.3,
	},
	AxisX: ABC{
		A0: 294.09,
		A1: 1.00829,
		B0: -1307.0,
		B1: 0.36,
		B2: 11.8e-03,
		B3: -15.0e-06,
		B4: -30.0e-08,
		C0: 1.301521,
		C1: 60.85e-06,
		C2: 665.7e-09,
		C3: -2481.0e-12,
		C4: 620.0e-14,
	},
	AxisY: ABC{
		A0: 292.794,
		A1: 1.01061,
		B0: -835.0,
		B1: -7.05,
		B2: 0.8e-03,
		B3: -17.0e-06,
		B4: 31.0e-08,
		C0: 1.301964,
		C1: 57.5e-06,
		C2: 758.8e-09,
		C3: -2608.0e-12,
		C4: 303.0e-14,
	},
	AxisZ: ABC{
		A0: 293.902,
		A1: 1.00191,
		B0: -290.0,
		B1: -1.11,
		B2: 8.5e-03,
		B3: -49.0e-06,
		B4: -76.0e-08,
		C0: 1.304559,
		C1: 64.0e-06,
		C2: 700.9e-09,
		C3: -2495.0e-12,
		C4: 464.0e-14,
	},
}

type Measurement struct {
	Record
	UPI string
	// When time.Time
	// Seq  uint16
	// Vid  uint32

	DegX float64
	DegY float64
	DegZ float64

	MicX float64
	MicY float64
	MicZ float64

	AccX []float64
	AccY []float64
	AccZ []float64

	ScaleX  float64
	OffsetX float64
	ScaleY  float64
	OffsetY float64
	ScaleZ  float64
	OffsetZ float64
}

type ABC struct {
	A0 float64 `toml:"A0"`
	A1 float64 `toml:"A1"`
	B0 float64 `toml:"B0"`
	B1 float64 `toml:"B1"`
	B2 float64 `toml:"B2"`
	B3 float64 `toml:"B3"`
	B4 float64 `toml:"B4"`
	C0 float64 `toml:"C0"`
	C1 float64 `toml:"C1"`
	C2 float64 `toml:"C2"`
	C3 float64 `toml:"C3"`
	C4 float64 `toml:"C4"`
}

func (a ABC) Temperatures(raw float64) (mica, celsius float64) {
	mica = (raw * 2.803e-03) + 272.48
	celsius = ((mica - a.A0) / a.A1) + TempMMA
	return
}

func (a ABC) ScaleFactor(scale, acc float64) float64 {
	factor := a.C0
	factor += a.C1 * acc
	factor += a.C2 * math.Pow(acc, 2)
	factor += a.C3 * math.Pow(acc, 3)
	factor += a.C4 * math.Pow(acc, 4)
	return scale * a.C0 / factor
}

func (a ABC) TempOffset(offset, acc float64) float64 {
	offset += a.B1 * acc
	offset += a.B2 * math.Pow(acc, 2)
	offset += a.B3 * math.Pow(acc, 3)
	offset += a.B4 * math.Pow(acc, 4)
	return offset
}

type XYZ struct {
	X float64 `toml:"X"`
	Y float64 `toml:"Y"`
	Z float64 `toml:"Z"`
}

type Table struct {
	Frequency int64

	Calib  XYZ `toml:"calibration"`
	Scale  XYZ
	Offset XYZ

	AxisX ABC `toml:"x-axis"`
	AxisY ABC `toml:"y-axis"`
	AxisZ ABC `toml:"z-axis"`
}

func (t *Table) Set(file string) error {
	return toml.DecodeFile(file, t)
}

func (t *Table) String() string {
	return "parameters table file"
}

func (t Table) SampleFrequency() float64 {
	return 1 / float64(t.Frequency)
}

func (t *Table) ScaleFactorX(a float64) float64 {
	return t.AxisX.ScaleFactor(t.Scale.X, a)
}

func (t *Table) ScaleFactorY(a float64) float64 {
	return t.AxisY.ScaleFactor(t.Scale.Y, a)
}

func (t *Table) ScaleFactorZ(a float64) float64 {
	return t.AxisZ.ScaleFactor(t.Scale.Z, a)
}

func (t *Table) TempOffsetX(a float64) float64 {
	return t.AxisX.TempOffset(t.Offset.X, a)
}

func (t *Table) TempOffsetY(a float64) float64 {
	return t.AxisY.TempOffset(t.Offset.Y, a)
}

func (t *Table) TempOffsetZ(a float64) float64 {
	return t.AxisZ.TempOffset(t.Offset.Z, a)
}

func (t *Table) Calibrate(file string) ([]Measurement, error) {
	raw, err := Convert(file, false)
	if err != nil {
		return nil, err
	}
	var (
		ms  []Measurement
		upi = splitFile(file)
	)
	for i := 0; i < len(raw); i++ {
		m := t.calibrate(raw[i])
		m.UPI = upi
		ms = append(ms, m)
	}
	return ms, nil
}

func (t *Table) calibrate(rec Record) Measurement {
	m := rec.Measurement()

	// temperatures in micro ampere (micXXX) and celsius (celXXX)
	m.MicX, m.DegX = t.AxisX.Temperatures(float64(m.Raw[0]))
	m.MicY, m.DegY = t.AxisY.Temperatures(float64(m.Raw[1]))
	m.MicZ, m.DegZ = t.AxisZ.Temperatures(float64(m.Raw[2]))

	// compute Ai
	var (
		ax = m.MicX + TempDelta
		ay = m.MicY + TempDelta
		az = m.MicZ + TempDelta
	)

	// compute Scale factor
	m.ScaleX = t.ScaleFactorX(ax)
	m.ScaleY = t.ScaleFactorY(ay)
	m.ScaleZ = t.ScaleFactorZ(az)

	// compute offset temperature
	m.OffsetX = t.TempOffsetX(ax)
	m.OffsetY = t.TempOffsetY(ay)
	m.OffsetZ = t.TempOffsetZ(az)

	m.AccX = apply(pick(m.Raw, 4), m.ScaleX, m.OffsetX)
	m.AccY = apply(pick(m.Raw, 5), m.ScaleY, m.OffsetY)
	m.AccZ = apply(pick(m.Raw, 6), m.ScaleZ, m.OffsetZ)

	return m
}

func Calibrate(file string) ([]Measurement, error) {
	return DefaultTable.Calibrate(file)
}

type Record struct {
	Seq    uint16
	Vid    uint32
	When   time.Time
	Raw    []int16
	NoDate bool
}

func (r Record) Measurement() Measurement {
	return Measurement{
		Record: r,
	}
}

func Convert(file string, duplicate bool) ([]Record, error) {
	mma, err := Open(file)
	if err != nil {
		return nil, err
	}
	var (
		sum  = adler32.New()
		rs   = bytes.NewReader(mma.Raw)
		tee  = io.TeeReader(rs, sum)
		seen = make(map[uint32]struct{})
		data []Record
	)
	for i := 0; rs.Len() > 0; i++ {
		var (
			raw [31]int16
			rec Record
		)
		if err := binary.Read(tee, binary.BigEndian, &rec.Seq); err != nil {
			return nil, err
		}
		if err := binary.Read(tee, binary.BigEndian, &raw); err != nil {
			return nil, err
		}
		cksum := sum.Sum32()
		if _, ok := seen[cksum]; duplicate || !ok {
			rec.Raw = append(rec.Raw, raw[:]...)
			rec.When = mma.When
			rec.Vid = mma.Vid
			data = insertRecord(data, rec)
			seen[cksum] = struct{}{}
		}
		sum.Reset()
	}
	return data, nil
}

const (
	AvgCount = 219
	MinDelta = -AvgCount * MeasCount
	MaxDelta = AvgCount * MeasCount
)

func insertRecord(data []Record, rec Record) []Record {
	z := len(data)
	if z == 0 {
		data = append(data, rec)
		return data
	}
	for i := z - 1; i >= 0; i-- {
		if data[i].NoDate {
			break
		}
		diff := int16(rec.Seq) - int16(data[i].Seq)
		if len(data) == 4 && (diff < MinDelta || diff > MaxDelta) {
			// data = append(make([]Record, 0, len(data)), rec)
			for i := 0; i < len(data); i++ {
				data[i].NoDate = true
			}
			data = append(data, rec)
			return data
		}
		if diff >= 0 {
			data = append(data[:i+1], append([]Record{rec}, data[i+1:]...)...)
			return data
		}
	}
	data = append([]Record{rec}, data...)
	return data
}

type MMA struct {
	Raw  []byte
	When time.Time
	Vid  uint32
}

func Open(file string) (MMA, error) {
	var m MMA
	r, err := os.Open(file)
	if err != nil {
		return m, err
	}
	defer r.Close()

	hdr := struct {
		FCC  [4]byte
		Seq  uint32
		Time int64
	}{}
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return m, err
	}
	if !bytes.Equal(hdr.FCC[:], Magic) {
		return m, fmt.Errorf("%s: invalid FCC", string(hdr.FCC[:]))
	}
	if m.Raw, err = ioutil.ReadAll(r); err != nil {
		return m, err
	}
	m.When = Epoch.Add(time.Duration(hdr.Time))
	m.Vid = hdr.Seq
	return m, err
}

func apply(values []float64, sf, off float64) []float64 {
	var vs []float64
	for _, v := range values {
		v = (v * sf) - off
		vs = append(vs, v)
	}
	return vs
}

func pick(values []int16, n int) []float64 {
	var vs []float64
	for i := n; i < len(values); i += 3 {
		vs = append(vs, float64(values[i]))
	}
	return vs
}

func splitFile(file string) string {
	parts := strings.Split(filepath.Base(file), "_")
	z := len(parts)
	if z == 0 {
		return ""
	}
	return strings.Join(parts[1:z-5], "_")
}
