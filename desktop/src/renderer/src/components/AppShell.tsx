export function AppShell({
  title,
  onClose,
  onMinimize,
  onMaximize,
  children,
}: {
  title: string;
  onClose: () => void;
  onMinimize: () => void;
  onMaximize: () => void;
  children: React.ReactNode;
}) {
  return (
    <main className="app-shell">
      <header className="titlebar">
        <div className="traffic-lights">
          <button className="window-control window-close" type="button" aria-label="Close window" onClick={onClose} />
          <button className="window-control window-minimize" type="button" aria-label="Minimize window" onClick={onMinimize} />
          <button className="window-control window-maximize" type="button" aria-label="Maximize window" onClick={onMaximize} />
        </div>
        <div className="titlebar-title">{title}</div>
      </header>
      {children}
    </main>
  );
}
