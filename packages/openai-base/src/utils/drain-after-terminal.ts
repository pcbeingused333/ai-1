/** Events after which a Responses stream has nothing more to send. */
const TERMINAL_EVENTS = new Set([
  'response.completed',
  'response.failed',
  'response.incomplete',
  'error',
])

/** How long the body may take to end by itself once a terminal event is read. */
export const DRAIN_AFTER_TERMINAL_MS = 250

interface AbortableStream<T> extends AsyncIterable<T> {
  controller?: { abort: () => void }
}

/**
 * Yields the events of `stream`. When the consumer stops after a terminal
 * event (the adapters do not wait for the HTTP body to close, #1445), the
 * rest of the body is read in the background and the request is aborted only
 * if it has not ended after `DRAIN_AFTER_TERMINAL_MS`. Aborting at once makes
 * the OpenAI SDK cancel a call that was already complete, and gateways log
 * that as a client cancel (499). A stream with no `controller` has nothing to
 * abort later, so it is closed at once. A stop on any other event, or an error, still
 * cancels the request straight away.
 */
export async function* drainAfterTerminal<T extends { type?: string }>(
  stream: AbortableStream<T>,
): AsyncGenerator<T> {
  const iterator = stream[Symbol.asyncIterator]()
  let ended = false
  let lastWasTerminal = false
  try {
    for (;;) {
      const step = await iterator.next()
      if (step.done) {
        ended = true
        return
      }
      lastWasTerminal = TERMINAL_EVENTS.has(step.value.type ?? '')
      yield step.value
    }
  } catch (error) {
    ended = true
    throw error
  } finally {
    if (!ended) {
      if (lastWasTerminal && stream.controller) {
        const { controller } = stream
        const timer = setTimeout(
          () => controller.abort(),
          DRAIN_AFTER_TERMINAL_MS,
        )
        void (async () => {
          try {
            for (;;) {
              if ((await iterator.next()).done) break
            }
          } catch {
            // the body failed after the response was complete, nothing to report
          } finally {
            clearTimeout(timer)
          }
        })()
      } else {
        await iterator.return?.()
      }
    }
  }
}
