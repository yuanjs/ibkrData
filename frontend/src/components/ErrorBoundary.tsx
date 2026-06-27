import { Component, type ErrorInfo, type ReactNode } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  error: Error | null
  componentStack: string
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, componentStack: '' }

  static getDerivedStateFromError(error: Error): State {
    return { error, componentStack: '' }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('UI render error:', error, info.componentStack)
    this.setState({ error, componentStack: info.componentStack ?? '' })
  }

  render() {
    if (this.state.error) {
      return (
        <div className="m-4 rounded border p-4 text-sm" style={{ borderColor: '#d32f2f', backgroundColor: 'var(--bg-surface)', color: 'var(--text-primary)' }}>
          <div className="mb-2 font-semibold" style={{ color: '#d32f2f' }}>界面渲染失败</div>
          <pre className="whitespace-pre-wrap break-words text-xs" style={{ color: 'var(--text-secondary)' }}>
            {this.state.error.stack ?? this.state.error.message}
            {this.state.componentStack ? `\n\nComponent stack:${this.state.componentStack}` : ''}
          </pre>
        </div>
      )
    }
    return this.props.children
  }
}
