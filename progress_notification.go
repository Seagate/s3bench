package main

import (
	"fmt"
	"time"
)

const (
	lineLen = 80
)

// ProgressNotifier is used to store current progress
type ProgressNotifier struct {
	name    string
	tbd     uint
	done    uint
	errors  uint
	started time.Time
	fillLen uint
}

func (prg *ProgressNotifier) showProgress() {
	donePrc := float64(prg.done) / float64(prg.tbd)
	curDur := time.Since(prg.started).Seconds()
	var eta float64
	if prg.done > 0 {
		eta = float64(curDur) / donePrc
	}
	var statusStr string
	if prg.done < prg.tbd {
		statusStr = fmt.Sprintf("%s | %d/%d (%.2f%%) | time %v eta %v | errors %d",
			prg.name,
			prg.done, prg.tbd, donePrc * 100,
			time.Duration(curDur) * time.Second, time.Duration(eta) * time.Second,
			prg.errors)
	} else {
		statusStr = fmt.Sprintf("%s done in %v with %d errors",
			prg.name, time.Duration(curDur) * time.Second, prg.errors)
	}

	fmt.Printf("\r%-*s", prg.fillLen, statusStr)
	if prg.done >= prg.tbd {
		fmt.Println()
	}
}

func startTestWithFill(testName string, total uint, flen uint) *ProgressNotifier {
	ret := &ProgressNotifier{
		name: testName,
		tbd: total,
		done: 0,
		errors: 0,
		started: time.Now(),
		fillLen: flen,
	}
	ret.showProgress()
	return ret
}

func startTest(testName string, total uint) *ProgressNotifier {
	return startTestWithFill(testName, total, lineLen)
}

func (prg *ProgressNotifier) updateProgress(prog uint, errs uint) {
	prg.done += prog
	prg.errors += errs
	prg.showProgress()
}
