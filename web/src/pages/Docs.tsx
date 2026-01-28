import CodeBlock from '../components/CodeBlock'

export default function Docs() {
  return (
    <main className="max-w-3xl mx-auto px-4 sm:px-6 py-16">
      <h1 className="text-2xl font-medium text-gray-900 mb-2">API Documentation</h1>
      <p className="text-gray-500 mb-8">
        RESTful API for querying user reputation and searching users.
      </p>

      <div className="mb-8 text-sm">
        <span className="text-gray-500">Base URL:</span>{' '}
        <code className="text-gray-700">https://fayan.jumble.social</code>
      </div>

      {/* GET /users/{pubkey} */}
      <section className="mb-12">
        <div className="flex items-center gap-2 mb-3">
          <span className="px-1.5 py-0.5 bg-green-50 text-green-700 text-xs font-medium rounded">GET</span>
          <code className="text-gray-900">/users/{'{pubkey}'}</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Query a single user's reputation information by pubkey or npub.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Path Parameters</h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24"><code>pubkey</code></td>
              <td className="py-2 text-gray-600">User's hex pubkey or npub</td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl https://fayan.jumble.social/users/82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">Response</h4>
        <CodeBlock
          code={`{
  "pubkey": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "rank": 2,
  "percentile": 99,
  "followers": 67525,
  "following": 693
}`}
        />
      </section>

      {/* POST /users */}
      <section className="mb-12">
        <div className="flex items-center gap-2 mb-3">
          <span className="px-1.5 py-0.5 bg-blue-50 text-blue-700 text-xs font-medium rounded">POST</span>
          <code className="text-gray-900">/users</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Batch query multiple users. Returns an object with pubkey as keys.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Request Body</h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24"><code>pubkeys</code></td>
              <td className="py-2 text-gray-600">Array of pubkeys (max 100)</td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl -X POST https://fayan.jumble.social/users \\
  -H "Content-Type: application/json" \\
  -d '{"pubkeys": ["82341f88...", "3bf0c63f..."]}'`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">Response</h4>
        <CodeBlock
          code={`{
  "82341f88...": {
    "pubkey": "82341f88...",
    "rank": 2,
    "percentile": 99,
    "followers": 67525,
    "following": 693
  },
  "3bf0c63f...": {
    "pubkey": "3bf0c63f...",
    "rank": 3,
    "percentile": 99,
    "followers": 45678,
    "following": 234
  }
}`}
        />
      </section>

      {/* GET /search */}
      <section className="mb-12">
        <div className="flex items-center gap-2 mb-3">
          <span className="px-1.5 py-0.5 bg-green-50 text-green-700 text-xs font-medium rounded">GET</span>
          <code className="text-gray-900">/search</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Search users by username or NIP-05. Returns an array with profile events.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Query Parameters</h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24"><code>q</code></td>
              <td className="py-2 text-gray-600">Search keyword (required)</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500"><code>limit</code></td>
              <td className="py-2 text-gray-600">Number of results (default 10, max 100)</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500"><code>offset</code></td>
              <td className="py-2 text-gray-600">Number of results to skip (default 0)</td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl "https://fayan.jumble.social/search?q=jack&limit=5"`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">Response</h4>
        <CodeBlock
          code={`[
  {
    "event": {
      "kind": 0,
      "id": "5b9f2400...",
      "pubkey": "82341f88...",
      "created_at": 1748690293,
      "content": "{\\"name\\":\\"jack\\",\\"picture\\":\\"https://...\\"}"
    },
    "pubkey": "82341f88...",
    "rank": 2,
    "percentile": 99,
    "followers": 67525,
    "following": 693
  }
]`}
        />
      </section>

      {/* Errors */}
      <section className="mb-12">
        <h2 className="text-lg font-medium text-gray-900 mb-4">Errors</h2>
        <table className="w-full text-sm">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-16"><code>400</code></td>
              <td className="py-2 text-gray-600">Invalid request parameters</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500"><code>404</code></td>
              <td className="py-2 text-gray-600">User not found</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500"><code>500</code></td>
              <td className="py-2 text-gray-600">Internal server error</td>
            </tr>
          </tbody>
        </table>
      </section>

      {/* Rate Limiting */}
      <section>
        <h2 className="text-lg font-medium text-gray-900 mb-4">Rate Limiting</h2>
        <ul className="text-sm text-gray-600 space-y-1">
          <li>60 requests per minute per IP</li>
          <li>100 pubkeys per batch query</li>
        </ul>
      </section>
    </main>
  )
}
