import CodeBlock from "../components/CodeBlock";

export default function Docs() {
  return (
    <main className="max-w-3xl mx-auto px-4 sm:px-6 py-16">
      <h1 className="text-2xl font-medium text-gray-900 mb-2">
        API Documentation
      </h1>
      <p className="text-gray-500 mb-8">
        RESTful API for querying user reputation and searching users.
      </p>

      <div className="mb-8 text-sm">
        <span className="text-gray-500">Base URL:</span>{" "}
        <code className="text-gray-700">https://fayan.jumble.social</code>
      </div>

      {/* GET /users/{pubkey} */}
      <section className="mb-12">
        <div className="flex items-center gap-2 mb-3">
          <span className="px-1.5 py-0.5 bg-green-50 text-green-700 text-xs font-medium rounded">
            GET
          </span>
          <code className="text-gray-900">/users/{"{pubkey}"}</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Query a single user's reputation information by pubkey or npub.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">
          Path Parameters
        </h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24">
                <code>pubkey</code>
              </td>
              <td className="py-2 text-gray-600">User's hex pubkey or npub</td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl https://fayan.jumble.social/users/82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">
          Response
        </h4>
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
          <span className="px-1.5 py-0.5 bg-blue-50 text-blue-700 text-xs font-medium rounded">
            POST
          </span>
          <code className="text-gray-900">/users</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Batch query multiple users. Returns an object with pubkey as keys.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Request Body</h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24">
                <code>pubkeys</code>
              </td>
              <td className="py-2 text-gray-600">Array of pubkeys (max 100)</td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl -X POST https://fayan.jumble.social/users \\
  -H "Content-Type: application/json" \\
  -d '{"pubkeys": ["82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2", "32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245"]}'`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">
          Response
        </h4>
        <CodeBlock
          code={`{
  "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2": {
    "pubkey": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
    "rank": 2,
    "percentile": 99,
    "followers": 67525,
    "following": 693
  },
  "32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245": {
    "pubkey": "32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245",
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
          <span className="px-1.5 py-0.5 bg-green-50 text-green-700 text-xs font-medium rounded">
            GET
          </span>
          <code className="text-gray-900">/search</code>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Search users by username or NIP-05. Returns an array with profile
          events.
        </p>

        <h4 className="text-sm font-medium text-gray-700 mb-2">
          Query Parameters
        </h4>
        <table className="w-full text-sm mb-4">
          <tbody>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500 w-24">
                <code>q</code>
              </td>
              <td className="py-2 text-gray-600">Search keyword (required)</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500">
                <code>limit</code>
              </td>
              <td className="py-2 text-gray-600">
                Number of results (default 10, max 100)
              </td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500">
                <code>offset</code>
              </td>
              <td className="py-2 text-gray-600">
                Number of results to skip (default 0)
              </td>
            </tr>
          </tbody>
        </table>

        <h4 className="text-sm font-medium text-gray-700 mb-2">Example</h4>
        <CodeBlock
          code={`curl "https://fayan.jumble.social/search?q=jack&limit=5"`}
          language="bash"
        />

        <h4 className="text-sm font-medium text-gray-700 mt-4 mb-2">
          Response
        </h4>
        <CodeBlock
          code={`[
  {
    "event": {
      "kind":0,
      "id":"5b9f240083555491a3acfd3df247e33317082d7285a101afd8a7ecc338b835bd",
      "pubkey":"82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
      "created_at":1748690293,
      "tags":[],
      "content":"{\"reactions\":false,\"about\":\"no state is the best state\",\"lud16\":\"jack@primal.net\",\"banner\":\"https://m.primal.net/IBZO.jpg\",\"website\":\"\",\"display_name\":\"\",\"name\":\"jack\",\"picture\":\"https://image.nostr.build/26867ce34e4b11f0a1d083114919a9f4eca699f3b007454c396ef48c43628315.jpg\"}",
      "sig":"a281b7011841e5c141af037668343a849c96ab5cbd1f206b3a361875e25cf39a6d9016d25b8075327e9d282de3de1c5632d3302c07da835ac7fe50e658227f76"
    },
    "pubkey": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
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
              <td className="py-2 pr-4 text-gray-500 w-16">
                <code>400</code>
              </td>
              <td className="py-2 text-gray-600">Invalid request parameters</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500">
                <code>404</code>
              </td>
              <td className="py-2 text-gray-600">User not found</td>
            </tr>
            <tr className="border-b border-gray-100">
              <td className="py-2 pr-4 text-gray-500">
                <code>500</code>
              </td>
              <td className="py-2 text-gray-600">Internal server error</td>
            </tr>
          </tbody>
        </table>
      </section>

      {/* Rate Limiting */}
      <section>
        <h2 className="text-lg font-medium text-gray-900 mb-4">
          Rate Limiting
        </h2>
        <ul className="text-sm text-gray-600 space-y-1">
          <li>60 requests per minute per IP</li>
          <li>100 pubkeys per batch query</li>
        </ul>
      </section>
    </main>
  );
}
