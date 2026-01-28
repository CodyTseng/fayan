import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'

export default function Header() {
  const location = useLocation()
  const [isMenuOpen, setIsMenuOpen] = useState(false)

  const navLinks = [
    { path: '/', label: 'Home' },
    { path: '/docs', label: 'API' },
  ]

  return (
    <header className="border-b border-gray-100">
      <div className="max-w-3xl mx-auto px-4 sm:px-6">
        <div className="flex justify-between items-center h-14">
          <Link to="/" className="text-lg font-medium text-gray-900">
            Fayan
          </Link>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex items-center space-x-6">
            {navLinks.map((link) => (
              <Link
                key={link.path}
                to={link.path}
                className={`text-sm transition-colors ${
                  location.pathname === link.path
                    ? 'text-gray-900'
                    : 'text-gray-500 hover:text-gray-900'
                }`}
              >
                {link.label}
              </Link>
            ))}
            <a
              href="https://github.com/CodyTseng/fayan"
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-gray-500 hover:text-gray-900 transition-colors"
            >
              GitHub
            </a>
          </nav>

          {/* Mobile menu button */}
          <button
            onClick={() => setIsMenuOpen(!isMenuOpen)}
            className="md:hidden p-1 text-gray-500 hover:text-gray-900"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              {isMenuOpen ? (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 6h16M4 12h16M4 18h16" />
              )}
            </svg>
          </button>
        </div>

        {/* Mobile Navigation */}
        {isMenuOpen && (
          <div className="md:hidden py-3 border-t border-gray-100">
            <nav className="flex flex-col space-y-2">
              {navLinks.map((link) => (
                <Link
                  key={link.path}
                  to={link.path}
                  onClick={() => setIsMenuOpen(false)}
                  className={`text-sm py-1 ${
                    location.pathname === link.path
                      ? 'text-gray-900'
                      : 'text-gray-500'
                  }`}
                >
                  {link.label}
                </Link>
              ))}
              <a
                href="https://github.com/CodyTseng/fayan"
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm py-1 text-gray-500"
              >
                GitHub
              </a>
            </nav>
          </div>
        )}
      </div>
    </header>
  )
}
