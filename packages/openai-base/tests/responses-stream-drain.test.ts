/**
 * After a terminal Responses event the adapters stop reading, which makes the
 * OpenAI SDK abort the request. A gateway then logs the finished call as a
 * client cancel (499). The adapters give the body a short time to end by
 * itself and only then abort it (#1965).
 */
import { describe, expect, it } from 'vitest'
import OpenAI from 'openai'
import { resolveDebugOption } from '@tanstack/ai/adapter-internals'
import { OpenAIBaseResponsesTextAdapter } from '../src/adapters/responses-text'
import type { ClientOptions } from 'openai'

type Fetch = NonNullable<ClientOptions['fetch']>

const logger = resolveDebugOption(false)
const model = 'gpt-5.5'
const completed = {
  type: 'response.completed',
  response: {
    id: 'response-1',
    model,
    status: 'completed',
    output: [
      {
        type: 'message',
        id: 'message-1',
        role: 'assistant',
        content: [{ type: 'output_text', text: '{"a":1}', annotations: [] }],
      },
    ],
  },
}

class Responses extends OpenAIBaseResponsesTextAdapter<string> {}

/** A body that sends `completed`, then ends after `closeAfterMs` (never if null). */
function fakeFetch(closeAfterMs: number | null, seen: { aborted: boolean }) {
  const fetch: Fetch = async (_url, init) => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          new TextEncoder().encode(`data: ${JSON.stringify(completed)}\n\n`),
        )
        if (closeAfterMs !== null) {
          setTimeout(() => {
            seen.aborted = init?.signal?.aborted ?? false
            try {
              controller.close()
            } catch {
              // the request was aborted, so the body is already closed
            }
          }, closeAfterMs)
        }
        init?.signal?.addEventListener('abort', () => {
          try {
            controller.error(new Error('aborted'))
          } catch {
            // already closed
          }
        })
      },
    })
    return new Response(body, {
      headers: { 'content-type': 'text/event-stream' },
    })
  }
  return fetch
}

function adapterFor(fetch: Fetch) {
  return new Responses(model, 'openai', new OpenAI({ apiKey: 'k', fetch }), {
    fetch,
  })
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const entries = [
  {
    name: 'chatStream',
    run: async (adapter: Responses) => {
      for await (const _ of adapter.chatStream({
        threadId: 't',
        logger,
        model,
        messages: [{ role: 'user', content: 'Hi' }],
      })) {
        // drain
      }
    },
  },
  {
    name: 'structuredOutputStream',
    run: async (adapter: Responses) => {
      for await (const _ of adapter.structuredOutputStream({
        chatOptions: {
          threadId: 't',
          logger,
          model,
          messages: [{ role: 'user', content: 'Hi' }],
        },
        outputSchema: { type: 'object', properties: { a: { type: 'number' } } },
      })) {
        // drain
      }
    },
  },
]

describe.each(entries)('$name after response.completed', ({ run }) => {
  it('does not abort a body that ends by itself soon after', async () => {
    const seen = { aborted: true }
    await run(adapterFor(fakeFetch(20, seen)))
    await sleep(60)
    expect(seen.aborted).toBe(false)
  })

  it('returns at once and aborts a body that never ends', async () => {
    const seen = { aborted: false }
    let signal: AbortSignal | undefined
    const inner = fakeFetch(null, seen)
    const fetch: Fetch = (url, init) => {
      signal = init?.signal ?? undefined
      return inner(url, init)
    }
    const started = Date.now()
    await run(adapterFor(fetch))
    expect(Date.now() - started).toBeLessThan(200)
    expect(signal?.aborted).toBe(false)
    await sleep(450)
    expect(signal?.aborted).toBe(true)
  })
})
