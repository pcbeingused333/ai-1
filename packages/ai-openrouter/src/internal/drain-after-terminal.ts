/** Events after which a Responses stream has nothing more to send. */
const TERMINAL_EVENTS = new Set([
  'response.completed',
  'response.failed',
  'response.incomplete',
  'error',
])

/** How long the body may take to end by itself once a terminal event is read. */
export const DRAIN_AFTER_TERMINAL_MS = 250

/** The SDK wraps an event it cannot parse as `UNKNOWN` and keeps the payload in `raw`. */
function eventType(event: { type?: string; raw?: unknown }): string {
  if (event.type === 'UNKNOWN') {
    const raw = event.raw as { type?: unknown } | null | undefined
    return typeof raw?.type === 'string' ? raw.type : ''
  }
  return event.type ?? ''
}

function hasReader<T>(
  stream: AsyncIterable<T>,
): stream is AsyncIterable<T> & ReadableStream<T> {
  return typeof (stream as Partial<ReadableStream<T>>).getReader === 'function'
}

/**
 * Yields the events of `stream`. When the consumer stops after a terminal
 * event (the adapter does not wait for the HTTP body to close, #1445), the
 * request is not cancelled at once. Leaving a `for await` loop cancels the
 * SDK's event stream and with it the fetch body, so a gateway logs a call that
 * was already complete as a client cancel (499). Instead the rest of the body
 * is read in the background and cancelled only if it has not ended after
 * `DRAIN_AFTER_TERMINAL_MS`. A stop on any other event, or a stream that is not
 * a `ReadableStream`, still cancels straight away.
 */
export async function* drainAfterTerminal<
  T extends { type?: string; raw?: unknown },
>(stream: AsyncIterable<T>): AsyncGenerator<T> {
  if (!hasReader(stream)) {
    yield* stream
    return
  }
  const reader = stream.getReader()
  let ended = false
  let lastWasTerminal = false
  try {
    for (;;) {
      const step = await reader.read()
      if (step.done) {
        ended = true
        return
      }
      lastWasTerminal = TERMINAL_EVENTS.has(eventType(step.value))
      yield step.value
    }
  } catch (error) {
    ended = true
    throw error
  } finally {
    if (ended) {
      reader.releaseLock()
    } else if (lastWasTerminal) {
      const timer = setTimeout(() => {
        reader.cancel().catch(() => {})
      }, DRAIN_AFTER_TERMINAL_MS)
      void (async () => {
        try {
          for (;;) {
            if ((await reader.read()).done) break
          }
        } catch {
          // the body failed after the response was complete, nothing to report
        } finally {
          clearTimeout(timer)
        }
      })()
    } else {
      await reader.cancel().catch(() => {})
    }
  }
}
