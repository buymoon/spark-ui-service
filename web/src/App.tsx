import "./styles.css";

export default function App() {
  return (
    <main className="app-shell">
      <header className="hero">
        <p className="eyebrow">Local Tool</p>
        <h1>Spark History UI Loader</h1>
        <p className="subtitle">
          Generate a .uimeta snapshot from a Spark eventlog and open the native Spark History UI.
        </p>
      </header>

      <section className="panel">
        <div className="tab-row">
          <button type="button" className="tab active">
            Upload File
          </button>
          <button type="button" className="tab">
            Local Path
          </button>
        </div>
        <p className="hint">Recommended for very large eventlogs: use Local Path mode.</p>
      </section>
    </main>
  );
}
