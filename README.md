[![Build Status](https://github.com/michael-diggin/workforce/workflows/Build/badge.svg?branch=main)](https://github.com/michael-diggin/workforce/actions)

# workforce (Work in Progress)
A resuable worker pool implementation in golang

Rather than creating and destroying a goroutine before and after it finishes a task, workforce allows you to reuse goroutines, and distribute repeated tasks to them without creating new workers. The worker pool gets created with a `maxWorkers` number of workers, and they are spun up with a context, allow the main thread or caller to cancel them and gracefully exit. 