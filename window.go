package saw

import (
	"sync"

	"golang.org/x/net/context"
)

type WindowSeqFunc func(datum Datum) int
type WindowFrameFactory func(name string, seq int) (Saw, error)

type WindowSpec struct {
	Name          string
	FrameFactory  WindowFrameFactory
	SeqFunc       WindowSeqFunc
	WindowSize    int
	MaxSeqAdvance int
}

type Window struct {
	spec WindowSpec

	mu        sync.Mutex
	frames    []Saw
	startSeq  int
	latestSeq int
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

func (win *Window) asyncFinalize(seq int, frame Saw) {
	win.finalizeWg.Add(1)
	go func() {
		frame.Result(context.Background())
		win.finalizeWg.Done()
	}()
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
	advance := seq - win.startSeq
	// Out of window
	if advance < 0 || (win.spec.MaxSeqAdvance > 0 && advance > win.spec.MaxSeqAdvance) {
		return nil, nil
	}
	if advance < len(win.frames) {
		idx := (win.startIdx + advance) % len(win.frames)
		if win.frames[idx] == nil {
			win.frames[idx], err = win.spec.FrameFactory(win.spec.Name, seq)
			if err != nil {
				return nil, err
			}
		}
		return win.frames[idx], nil
	}
	frame, err = win.spec.FrameFactory(win.spec.Name, seq)
	if err != nil {
		return
	}
	if seq > win.latestSeq {
		win.latestSeq = seq
	}
	if advance >= len(win.frames)*2 {
		for i := 0; i < len(win.frames); i++ {
			frameIdx := (win.startIdx + i) % len(win.frames)
			frame := win.frames[frameIdx]
			if frame != nil {
				win.frames[frameIdx] = nil
				win.asyncFinalize(win.startSeq+i, frame)
			}
		}
		win.startSeq = seq - len(win.frames) + 1
		win.startIdx = 0
		win.frames[len(win.frames)-1] = frame
		return frame, nil
	} else {
		for i := 0; i < advance-len(win.frames); i++ {
			if win.frames[win.startIdx] != nil {
				win.asyncFinalize(win.startSeq, win.frames[win.startIdx])
				win.frames[win.startIdx] = nil
			}
			win.startIdx = (win.startIdx + 1) % len(win.frames)
			win.startSeq++
		}
		win.frames[(win.startIdx+(seq-win.startSeq))%len(win.frames)] = frame
		return frame, nil
	}
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

func (win *Window) Result(ctx context.Context) (result interface{}, err error) {
	win.mu.Lock()
	defer win.mu.Unlock()
	for i := 0; i < len(win.frames); i++ {
		frameIdx := (win.startIdx + i) % len(win.frames)
		frame := win.frames[frameIdx]
		if frame != nil {
			win.frames[frameIdx] = nil
			win.asyncFinalize(win.startSeq+i, frame)
		}
	}
	win.startSeq = 0
	win.startIdx = 0
	win.latestSeq = 0
	win.hasData = false
	win.finalizeWg.Wait()
	return nil, nil
}

func (win *Window) LatestFrame() (seq int, frame Saw) {
	win.mu.Lock()
	defer win.mu.Unlock()

	if !win.hasData {
		return 0, nil
	}
	return win.latestSeq, win.frames[(win.startIdx+win.latestSeq-win.startSeq)%len(win.frames)]
}

func (win *Window) AllFrames() map[int]Saw {
	win.mu.Lock()
	defer win.mu.Unlock()
	output := make(map[int]Saw)
	if !win.hasData {
		return output
	}
	for i := 0; i < len(win.frames); i++ {
		frame := win.frames[(win.startIdx+i)%len(win.frames)]
		if frame != nil {
			output[win.startSeq+i] = frame
		}
	}
	return output
}
