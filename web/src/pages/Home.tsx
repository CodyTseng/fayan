import { useState, useRef, useCallback } from 'react'
import SearchBox from '../components/SearchBox'
import UserCard from '../components/UserCard'
import { searchUsers, getUser, type User } from '../api/client'

const PAGE_SIZE = 20

export default function Home() {
  const [users, setUsers] = useState<User[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [hasSearched, setHasSearched] = useState(false)
  const [hasMore, setHasMore] = useState(false)
  const queryRef = useRef('')

  const handleSearch = useCallback(async (query: string) => {
    if (!query) {
      setUsers([])
      setError(null)
      setHasSearched(false)
      setHasMore(false)
      queryRef.current = ''
      return
    }

    setIsLoading(true)
    setError(null)
    setHasSearched(true)
    setHasMore(false)
    queryRef.current = query

    try {
      const isPubkeyLike = /^[0-9a-f]{64}$/i.test(query) || query.startsWith('npub1')

      if (isPubkeyLike) {
        const user = await getUser(query)
        setUsers(user ? [user] : [])
        if (!user) {
          setError('User not found')
        }
      } else {
        const result = await searchUsers(query, PAGE_SIZE, 0)
        setUsers(result)
        setHasMore(result.length === PAGE_SIZE)
        if (result.length === 0) {
          setError('No matching users found')
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed, please try again')
      setUsers([])
    } finally {
      setIsLoading(false)
    }
  }, [])

  const handleLoadMore = async () => {
    if (isLoadingMore || !hasMore) return

    setIsLoadingMore(true)
    try {
      const result = await searchUsers(queryRef.current, PAGE_SIZE, users.length)
      setUsers((prev) => [...prev, ...result])
      setHasMore(result.length === PAGE_SIZE)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load more')
    } finally {
      setIsLoadingMore(false)
    }
  }

  return (
    <main className="max-w-3xl mx-auto px-4 sm:px-6 py-16">
      {/* Hero */}
      <div className="mb-10">
        <h1 className="text-2xl font-medium text-gray-900 mb-2">
          Fayan
        </h1>
        <p className="text-gray-500">
          Nostr reputation system based on social graph analysis.
        </p>
      </div>

      {/* Search */}
      <div className="mb-8">
        <SearchBox
          onSearch={handleSearch}
          isLoading={isLoading}
          error={error}
        />
      </div>

      {/* Results */}
      {hasSearched && !isLoading && users.length > 0 && (
        <div>
          <div className="text-sm text-gray-400 mb-2">
            {users.length} result{users.length > 1 ? 's' : ''}
          </div>
          <div>
            {users.map((user) => (
              <UserCard key={user.pubkey} user={user} />
            ))}
          </div>
          {hasMore && (
            <button
              onClick={handleLoadMore}
              disabled={isLoadingMore}
              className="mt-4 w-full py-2 text-sm text-gray-500 hover:text-gray-700 border border-gray-200 rounded-md hover:border-gray-300 disabled:opacity-50 transition-colors"
            >
              {isLoadingMore ? 'Loading...' : 'Load more'}
            </button>
          )}
        </div>
      )}

      {/* Empty State */}
      {hasSearched && !isLoading && users.length === 0 && !error && (
        <div className="text-center py-12 text-gray-400">
          No results found
        </div>
      )}

      {/* Features */}
      {!hasSearched && (
        <div className="mt-16 pt-8 border-t border-gray-100">
          <div className="grid md:grid-cols-3 gap-8 text-sm">
            <div>
              <h3 className="font-medium text-gray-900 mb-1">Reputation Ranking</h3>
              <p className="text-gray-500">
                PageRank-based influence scoring for Nostr users.
              </p>
            </div>
            <div>
              <h3 className="font-medium text-gray-900 mb-1">Quick Search</h3>
              <p className="text-gray-500">
                Search by username, pubkey, or npub.
              </p>
            </div>
            <div>
              <h3 className="font-medium text-gray-900 mb-1">Open API</h3>
              <p className="text-gray-500">
                RESTful API for integration.
              </p>
            </div>
          </div>
        </div>
      )}
    </main>
  )
}
