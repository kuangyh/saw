package saw

import (
	"sync"

	"golang.org/x/net/context"
)

type SeqID int

func (seq SeqID) Advance(x int) SeqID {
	return SeqID(int(seq) + x)
}

func (seq SeqID) DistanceFrom(other SeqID) int {
	return int(seq - other)
}

type DatumSeqFunc func(datum Datum) SeqID
type WindowFrameFactory func(name string, seq SeqID) (Saw, error)

type WindowSpec struct {
	Name          string
	FrameFactory  WindowFrameFactory
	SeqFunc       DatumSeqFunc
	WindowSize    int
	MaxSeqAdvance int
}

// Window implements a sliding window of saws. Window keeps finite set of frames,
// each corresponded with a SeqID. Window keeps WindowSpec.WindowSize of latest
// frames with largest, contintued SeqID.
//
// For every input, it calls WindowSpec.SeqFunc to get SeqID of a datum and route
// it to the frame if it's still in sliding window. Window assumes datum's SeqID
// are dense, rougly incremental overtime, or you will get too many datums dropped.
//
// When window needed to be slided, Result() of old frames will be called in their
// seperate gorountines. In Window.Result(), all frames' Result() will be called
// in this maner as they all been slide away. Window doesn't care frame's Result()
// return.
type Window struct {
	spec WindowSpec

	mu        sync.Mutex
	frames    []Saw
	startSeq  SeqID
	latestSeq SeqID
	startIdx  int
	hasData   bool

	finalizeWg sync.WaitGroup

	droppedCount VarInt
}

func NewWindow(spec WindowSpec) *Window {
	return &Window{
		spec:         spec,
		frames:       make([]Saw, spec.WindowSize),
		droppedCount: ReportInt(spec.Name, "droppedCount"),
	}
}

func (win *Window) asyncFinalize(seq SeqID, frame Saw) {
	win.finalizeWg.Add(1)
	go func() {
		frame.Result(context.Background())
		win.finalizeWg.Done()
	}()
}

func (win *Window) indexForSeq(seq SeqID) int {
	return (win.startIdx + seq.DistanceFrom(win.startSeq)) % len(win.frames)
}

func (win *Window) indexForOffset(offset int) int {
	return (win.startIdx + offset) % len(win.frames)
}

// returning Saw nullable indicating drop
func (win *Window) prepareFrame(datum Datum) (frame Saw, err error) {
	seq := win.spec.SeqFunc(datum)
	win.mu.Lock()
	defer win.mu.Unlock()
	if !win.hasData {
		frame, err = win.spec.FrameFactory(win.spec.Name, seq)
		if err != nil {
			return
		}
		win.startSeq = seq
		win.latestSeq = seq
		win.startIdx = 0
		win.frames[win.startIdx] = frame
		win.hasData = true
		return frame, nil
	}
	offset := seq.DistanceFrom(win.startSeq)
	// Out of window, drop
	if offset < 0 || (win.spec.MaxSeqAdvance > 0 && offset > win.spec.MaxSeqAdvance) {
		win.droppedCount.Add(1)
		return nil, nil
	}
	winSize := len(win.frames)
	if offset < winSize {
		frameIdx := win.indexForOffset(offset)
		if win.frames[frameIdx] == nil {
			win.frames[frameIdx], err = win.spec.FrameFactory(win.spec.Name, seq)
			if err != nil {
				return nil, err
			}
		}
		return win.frames[frameIdx], nil
	}
	frame, err = win.spec.FrameFactory(win.spec.Name, seq)
	if err != nil {
		return
	}
	if seq > win.latestSeq {
		win.latestSeq = seq
	}
	if offset >= winSize*2 {
		for i := 0; i < winSize; i++ {
			frameIdx := win.indexForOffset(i)
			frame := win.frames[frameIdx]
			if frame != nil {
				win.frames[frameIdx] = nil
				win.asyncFinalize(win.startSeq.Advance(i), frame)
			}
		}
		win.startSeq = seq.Advance(1 - winSize)
		win.startIdx = 0
	} else {
		for i := 0; i < offset-winSize; i++ {
			if win.frames[win.startIdx] != nil {
				win.asyncFinalize(win.startSeq, win.frames[win.startIdx])
				win.frames[win.startIdx] = nil
			}
			win.startIdx = win.indexForOffset(1)
			win.startSeq++
		}
	}
	win.frames[win.indexForSeq(seq)] = frame
	return frame, nil
}

func (win *Window) Emit(datum Datum) error {
	frame, err := win.prepareFrame(datum)
	if err != nil {
		return err
	}
	if frame == nil {
		win.droppedCount.Add(1)
		return nil
	}
	return frame.Emit(datum)
}

// Result finalize all frames it's currently managing, returns after all frames
// sent for finalize finishes, including previous ones caused by sliding.
func (win *Window) Result(ctx context.Context) (result interface{}, err error) {
	win.mu.Lock()
	defer win.mu.Unlock()
	for i := 0; i < len(win.frames); i++ {
		frameIdx := win.indexForOffset(i)
		frame := win.frames[frameIdx]
		if frame != nil {
			win.frames[frameIdx] = nil
			win.asyncFinalize(win.startSeq.Advance(i), frame)
		}
	}
	win.startSeq = 0
	win.startIdx = 0
	win.latestSeq = 0
	win.hasData = false
	win.finalizeWg.Wait()
	return nil, nil
}

// Gets the latest frame or nil when there's no data yet. returned frame is not
// locked, would be Emit()-ing or even Result()-ing when caller gets the return.
func (win *Window) LatestFrame() (seq SeqID, frame Saw) {
	win.mu.Lock()
	defer win.mu.Unlock()

	if !win.hasData {
		return 0, nil
	}
	return win.latestSeq, win.frames[win.indexForSeq(win.latestSeq)]
}

// Gets the all frame the Window currently holds. returned frames are not locked,
// would be Emit()-ing or even Result()-ing when caller gets the return.
func (win *Window) AllFrames() map[SeqID]Saw {
	win.mu.Lock()
	defer win.mu.Unlock()
	output := make(map[SeqID]Saw)
	if !win.hasData {
		return output
	}
	for i := 0; i < len(win.frames); i++ {
		frame := win.frames[win.indexForOffset(i)]
		if frame != nil {
			output[win.startSeq.Advance(i)] = frame
		}
	}
	return output
}
