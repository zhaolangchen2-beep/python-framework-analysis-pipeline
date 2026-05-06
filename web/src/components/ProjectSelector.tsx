import { useProject } from "../context/ProjectContext";
import { setCurrentProjectId } from "../data/loaders";
import "../styles/ProjectSelector.css";

export function ProjectSelector() {
  const { projectId, setProjectId, availableProjects } = useProject();

  const handleProjectChange = (newProjectId: string) => {
    const id = newProjectId as "tpch-pyflink-reference" | "pyspark-tpch-reference";
    setProjectId(id);
    setCurrentProjectId(id);
    window.location.reload();
  };

  return (
    <div className="project-selector">
      <label htmlFor="project-select">Framework:</label>
      <select
        id="project-select"
        value={projectId}
        onChange={(e) => handleProjectChange(e.target.value)}
        className="project-select"
      >
        {availableProjects.map((project) => (
          <option key={project.id} value={project.id}>
            {project.label} - {project.description}
          </option>
        ))}
      </select>
    </div>
  );
}
