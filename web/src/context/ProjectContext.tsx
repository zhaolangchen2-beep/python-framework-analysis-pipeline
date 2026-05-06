import { createContext, useContext, useState, ReactNode } from "react";

export type ProjectId = "tpch-pyflink-reference" | "pyspark-tpch-reference";

interface ProjectContextType {
  projectId: ProjectId;
  setProjectId: (id: ProjectId) => void;
  getProjectLabel: (id: ProjectId) => string;
  availableProjects: Array<{ id: ProjectId; label: string; description: string }>;
}

const ProjectContext = createContext<ProjectContextType | undefined>(undefined);

const AVAILABLE_PROJECTS = [
  {
    id: "tpch-pyflink-reference" as const,
    label: "PyFlink",
    description: "Apache PyFlink framework analysis (TPC-H reference)",
  },
  {
    id: "pyspark-tpch-reference" as const,
    label: "PySpark",
    description: "Apache PySpark framework analysis (TPC-H reference)",
  },
];

export function ProjectProvider({ children }: { children: ReactNode }) {
  const [projectId, setProjectId] = useState<ProjectId>("pyspark-tpch-reference");

  const getProjectLabel = (id: ProjectId): string => {
    const project = AVAILABLE_PROJECTS.find((p) => p.id === id);
    return project?.label || id;
  };

  return (
    <ProjectContext.Provider
      value={{
        projectId,
        setProjectId,
        getProjectLabel,
        availableProjects: AVAILABLE_PROJECTS,
      }}
    >
      {children}
    </ProjectContext.Provider>
  );
}

export function useProject(): ProjectContextType {
  const context = useContext(ProjectContext);
  if (!context) {
    throw new Error("useProject must be used within ProjectProvider");
  }
  return context;
}
