# Goal:  Rewrite Rxjs Operator names into custom human readable sensible names

## map user friendly description - apply a transformation function to each element in the list
   map user friendly short name - transformWith

## filter user friendly description - apply a predicate function to each element that emits only those values that satisfy the predicate
   filter user friendly short name - keepIf

Join Creation Operators
These are Observable creation operators that also have join functionality -- emitting values of multiple source Observables.

## combineLatest user friendly description - combines multiple observables and emits an array of their latest values whenever any one emits
   combineLatest user friendly short name - latestFromAll

## concat user friendly description - runs observables one after another, waiting for each to complete before starting the next
   concat user friendly short name - runInSequence

## forkJoin user friendly description - waits for all observables to complete, then emits an array of their last values
   forkJoin user friendly short name - waitForAll

## merge user friendly description - subscribes to all observables at once and emits values from whichever emits first
   merge user friendly short name - mergeAll

## partition user friendly description - splits an observable into two based on a predicate: one for matching values, one for the rest
   partition user friendly short name - splitBy

## race user friendly description - subscribes to all observables and mirrors only the one that emits first
   race user friendly short name - firstToEmit

## zip user friendly description - combines observables by pairing their nth emissions together into arrays
   zip user friendly short name - pairEmissions

Transformation Operators

## buffer user friendly description - collects emitted values into arrays, releasing each batch when a signal observable emits
   buffer user friendly short name - collectUntilSignal

## bufferCount user friendly description - collects emitted values into fixed-size arrays and emits each full batch
   bufferCount user friendly short name - collectN

## bufferTime user friendly description - collects emitted values into arrays over fixed time windows and emits each batch
   bufferTime user friendly short name - collectForDuration

## bufferToggle user friendly description - opens a collection window when one observable emits and closes it when another emits
   bufferToggle user friendly short name - collectBetweenSignals

## bufferWhen user friendly description - opens a collection window immediately and closes it when a factory observable emits
   bufferWhen user friendly short name - collectUntilFactory

## concatMap user friendly description - maps each value to an observable and waits for it to complete before processing the next
   concatMap user friendly short name - transformSequentially

## concatMapTo user friendly description - maps every value to the same observable and subscribes to them one at a time in order
   concatMapTo user friendly short name - switchToSequentially

## exhaust user friendly description - subscribes to the first inner observable and ignores new ones until it completes
   exhaust user friendly short name - ignoreWhileBusy

## exhaustMap user friendly description - maps each value to an observable but ignores new values while the current inner observable is active
   exhaustMap user friendly short name - transformIfNotBusy

## expand user friendly description - recursively applies a function to each emitted value and merges the results
   expand user friendly short name - expandRecursively

## groupBy user friendly description - groups emitted values by a key function, emitting a grouped observable per key
   groupBy user friendly short name - groupInto

## mapTo user friendly description - replaces every emitted value with a fixed constant value
   mapTo user friendly short name - replaceWith

## mergeMap user friendly description - maps each value to an observable and merges all inner observables concurrently
   mergeMap user friendly short name - transformConcurrently

## mergeMapTo user friendly description - maps every value to the same observable and merges all subscriptions concurrently
   mergeMapTo user friendly short name - mergeInto

## mergeScan user friendly description - accumulates values like scan but merges concurrent inner observables into the result
   mergeScan user friendly short name - accumulateConcurrently

## pairwise user friendly description - emits the current and previous value together as a pair on each emission
   pairwise user friendly short name - withPrevious

## partition user friendly description - splits an observable into two based on a predicate: matching values and the rest
   partition user friendly short name - splitBy

## pluck user friendly description - extracts a single named property from each emitted object
   pluck user friendly short name - pickProperty

## scan user friendly description - accumulates values over time by applying a function to each emission and the running result
   scan user friendly short name - accumulate

## switchScan user friendly description - like scan but switches to the latest inner observable on each emission, cancelling the previous
   switchScan user friendly short name - accumulateSwitching

## switchMap user friendly description - maps each value to an observable and cancels the previous inner observable when a new value arrives
   switchMap user friendly short name - transformSwitching

## switchMapTo user friendly description - maps every value to the same observable and cancels the previous subscription on each new value
   switchMapTo user friendly short name - switchTo

## window user friendly description - groups emitted values into observable windows, opening a new window when a signal emits
   window user friendly short name - windowUntilSignal

## windowCount user friendly description - groups emitted values into fixed-size observable windows
   windowCount user friendly short name - windowOfN

## windowTime user friendly description - groups emitted values into observable windows over fixed time intervals
   windowTime user friendly short name - windowForDuration

## windowToggle user friendly description - opens observable windows when one signal emits and closes them when another emits
   windowToggle user friendly short name - windowBetweenSignals

## windowWhen user friendly description - opens an observable window immediately and closes it when a factory observable emits
   windowWhen user friendly short name - windowUntilFactory

## Filtering Operators

## audit user friendly description - emits the most recent value from the source after a silence determined by a signal observable
   audit user friendly short name - emitLatestAfterSignal

## auditTime user friendly description - emits the most recent value from the source after a fixed silence period
   auditTime user friendly short name - emitLatestAfterDelay

## debounce user friendly description - emits a value only after the source is silent for a duration determined by a signal observable
   debounce user friendly short name - afterSilence

## debounceTime user friendly description - emits a value only after the source is silent for a fixed duration
   debounceTime user friendly short name - afterSilenceOf

## distinct user friendly description - emits only values that have not been seen before
   distinct user friendly short name - uniqueValues

## distinctUntilChanged user friendly description - emits a value only when it is different from the immediately previous value
   distinctUntilChanged user friendly short name - skipDuplicates

## distinctUntilKeyChanged user friendly description - emits a value only when a specific property differs from the same property on the previous value
   distinctUntilKeyChanged user friendly short name - skipDuplicatesBy

## elementAt user friendly description - emits only the value at a specific index position in the sequence
   elementAt user friendly short name - atIndex

## filter user friendly description - emits only values that satisfy a predicate function
   filter user friendly short name - keepIf

## first user friendly description - emits only the first value, or the first value that satisfies an optional predicate
   first user friendly short name - takeFirst

## ignoreElements user friendly description - suppresses all emitted values, passing through only errors and completion
   ignoreElements user friendly short name - suppressValues

## last user friendly description - emits only the last value, or the last value that satisfies an optional predicate
   last user friendly short name - takeLast

## sample user friendly description - emits the most recent source value whenever a separate signal observable emits
   sample user friendly short name - snapshotOn

## sampleTime user friendly description - emits the most recent source value at regular time intervals
   sampleTime user friendly short name - snapshotEvery

## single user friendly description - emits the one value that satisfies a predicate, erroring if there is none or more than one
   single user friendly short name - exactlyOne

## skip user friendly description - ignores the first N emitted values and then passes through the rest
   skip user friendly short name - dropFirst

## skipLast user friendly description - emits all values except the last N
   skipLast user friendly short name - dropLast

## skipUntil user friendly description - ignores emitted values until a signal observable emits, then passes through the rest
   skipUntil user friendly short name - startAfterSignal

## skipWhile user friendly description - ignores emitted values while a predicate holds true, then passes through all subsequent values
   skipWhile user friendly short name - dropWhile

## take user friendly description - emits only the first N values and then completes
   take user friendly short name - limitTo

## takeLast user friendly description - waits for the source to complete and then emits only the last N values
   takeLast user friendly short name - lastN

## takeUntil user friendly description - emits values until a signal observable emits, then completes
   takeUntil user friendly short name - stopWhen

## takeWhile user friendly description - emits values while a predicate holds true, then completes
   takeWhile user friendly short name - stopWhenNot

## throttle user friendly description - emits a value then ignores subsequent values for a duration determined by a signal observable
   throttle user friendly short name - limitRate

## throttleTime user friendly description - emits a value then ignores subsequent values for a fixed duration
   throttleTime user friendly short name - limitRateTo

Join Operators
Also see the Join Creation Operators section above.

## combineLatestAll user friendly description - flattens an observable of observables by combining their latest values into arrays
   combineLatestAll user friendly short name - combineAllLatest

## concatAll user friendly description - flattens an observable of observables by subscribing to each one after the previous completes
   concatAll user friendly short name - joinSequentially

## exhaustAll user friendly description - flattens an observable of observables by subscribing to the first and ignoring new ones while it is active
   exhaustAll user friendly short name - joinIfNotBusy

## mergeAll user friendly description - flattens an observable of observables by subscribing to all of them concurrently
   mergeAll user friendly short name - joinConcurrently

## switchAll user friendly description - flattens an observable of observables by always switching to the latest inner observable
   switchAll user friendly short name - joinSwitching

## startWith user friendly description - emits specified values first before emitting values from the source
   startWith user friendly short name - prependWith

## withLatestFrom user friendly description - pairs each source value with the most recent value from another observable
   withLatestFrom user friendly short name - pairWithLatest

Multicasting Operators

## multicast user friendly description - shares a single subscription to the source through a provided Subject
   multicast user friendly short name - shareVia

## publish user friendly description - converts an observable into a connectable one that multicasts to all subscribers at once
   publish user friendly short name - makeHot

## publishBehavior user friendly description - like publish but uses a BehaviorSubject so new subscribers immediately receive the latest value
   publishBehavior user friendly short name - makeHotWithLatest

## publishLast user friendly description - like publish but uses an AsyncSubject so subscribers receive only the last value on completion
   publishLast user friendly short name - makeHotWithLastValue

## publishReplay user friendly description - like publish but uses a ReplaySubject so new subscribers receive buffered past values
   publishReplay user friendly short name - makeHotWithReplay

## share user friendly description - multicasts to multiple subscribers using a single underlying subscription, resetting when subscriber count drops to zero
   share user friendly short name - shareAmong

Error Handling Operators

## catchError user friendly description - catches errors and returns a fallback observable or rethrows
   catchError user friendly short name - handleError

## retry user friendly description - resubscribes to the source on error, up to a specified number of times
   retry user friendly short name - tryAgain

## retryWhen user friendly description - resubscribes to the source based on a notifier observable that controls retry timing
   retryWhen user friendly short name - retryWhenSignaled

Utility Operators

## tap user friendly description - performs a side effect for each emission without modifying the values
   tap user friendly short name - sideEffect

## delay user friendly description - shifts all emissions forward in time by a fixed duration
   delay user friendly short name - delayBy

## delayWhen user friendly description - shifts each emission forward in time by a duration determined by a per-value signal observable
   delayWhen user friendly short name - delayUntilSignal

## dematerialize user friendly description - converts Notification objects back into ordinary observable emissions, errors, and completions
   dematerialize user friendly short name - unwrapNotification

## materialize user friendly description - wraps every emission, error, and completion into a Notification object
   materialize user friendly short name - wrapAsNotification

## observeOn user friendly description - re-emits values on a specified scheduler, controlling which thread or queue handles delivery
   observeOn user friendly short name - scheduleOn

## subscribeOn user friendly description - controls the scheduler used when subscribing to the source observable
   subscribeOn user friendly short name - subscribeUsing

## timeInterval user friendly description - emits an object containing each value and the elapsed time since the previous emission
   timeInterval user friendly short name - withTimeInterval

## timestamp user friendly description - emits an object containing each value and the wall-clock time at which it was emitted
   timestamp user friendly short name - withTimestamp

## timeout user friendly description - errors if the source does not emit within a specified time limit
   timeout user friendly short name - failAfter

## timeoutWith user friendly description - switches to a fallback observable if the source does not emit within a specified time limit
   timeoutWith user friendly short name - fallbackAfter

## toArray user friendly description - collects all emitted values into a single array and emits it when the source completes
   toArray user friendly short name - collectAll

Conditional and Boolean Operators

## defaultIfEmpty user friendly description - emits a specified default value if the source completes without emitting anything
   defaultIfEmpty user friendly short name - orDefault

## every user friendly description - emits true if all source values satisfy a predicate, false as soon as one does not
   every user friendly short name - allMatch

## find user friendly description - emits the first value that satisfies a predicate, then completes
   find user friendly short name - firstMatch

## findIndex user friendly description - emits the index of the first value that satisfies a predicate, then completes
   findIndex user friendly short name - firstMatchIndex

## isEmpty user friendly description - emits true if the source completes without emitting any values, otherwise false
   isEmpty user friendly short name - hasNoValues

Mathematical and Aggregate Operators

## count user friendly description - counts the number of values emitted by the source and emits the total on completion
   count user friendly short name - countValues

## max user friendly description - waits for the source to complete and emits the largest value
   max user friendly short name - maximum

## min user friendly description - waits for the source to complete and emits the smallest value
   min user friendly short name - minimum

## reduce user friendly description - applies an accumulator function over all source values and emits the final result on completion
   reduce user friendly short name - foldInto
