import { ProjectSelector } from "../components/ProjectSelector";

export function TopBar() {
  return (
    <header className="top-bar">
      <div className="top-bar__content">
        <div>
          <p className="eyebrow">Python 框架分析流程</p>
          <h1>交互式分析 Demo</h1>
        </div>
        <ProjectSelector />
      </div>
    </header>
  );
}
