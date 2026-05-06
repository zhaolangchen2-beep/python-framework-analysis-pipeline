import {
  assembleArtifactDetail as assembleArtifactDetailFromAssembly,
  assembleCaseDetail as assembleCaseDetailFromAssembly,
  assembleCaseIndex as assembleCaseIndexFromAssembly,
  assembleCategoryDetail as assembleCategoryDetailFromAssembly,
  assembleComponentDetail as assembleComponentDetailFromAssembly,
  assembleExecutiveSummary as assembleExecutiveSummaryFromAssembly,
  assembleFunctionDetail as assembleFunctionDetailFromAssembly,
  assembleOpportunityRanking as assembleOpportunityRankingFromAssembly,
  assemblePatternDetail as assemblePatternDetailFromAssembly,
  assemblePatternIndex as assemblePatternIndexFromAssembly,
  assembleRootCauseDetail as assembleRootCauseDetailFromAssembly,
  assembleRootCauseIndex as assembleRootCauseIndexFromAssembly,
  assembleScopeSummary as assembleScopeSummaryFromAssembly,
  assembleStackOverview as assembleStackOverviewFromAssembly,
  loadAssemblyContext,
} from "./assembly";
import type {
  ArtifactDetail,
  CaseIndexEntry,
  CaseDetail,
  CategoryDetail,
  ComponentDetail,
  ExecutiveSummary,
  FunctionDetail,
  OpportunityRankingEntry,
  PatternDetail,
  PatternIndexEntry,
  RootCauseIndexEntry,
  RootCauseDetail,
  ScopeSummary,
  StackOverview,
} from "../types/report";

export type ProjectId = "tpch-pyflink-reference" | "pyspark-tpch-reference";

const DEFAULT_PROJECT_ID: ProjectId = "pyspark-tpch-reference";
let currentProjectId: ProjectId = DEFAULT_PROJECT_ID;

export function setCurrentProjectId(projectId: ProjectId) {
  currentProjectId = projectId;
}

export function getCurrentProjectId(): ProjectId {
  return currentProjectId;
}

async function loadAssemblyContextForProject(projectId?: ProjectId) {
  const id = projectId || currentProjectId;
  return loadAssemblyContext(id);
}

export async function loadExecutiveSummary(projectId?: ProjectId): Promise<ExecutiveSummary> {
  return assembleExecutiveSummaryFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadCaseIndex(projectId?: ProjectId): Promise<CaseIndexEntry[]> {
  return assembleCaseIndexFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadOpportunityRanking(projectId?: ProjectId): Promise<OpportunityRankingEntry[]> {
  return assembleOpportunityRankingFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadScopeSummary(projectId?: ProjectId): Promise<ScopeSummary> {
  return assembleScopeSummaryFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadStackOverview(projectId?: ProjectId): Promise<StackOverview> {
  return assembleStackOverviewFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadRootCauseIndex(projectId?: ProjectId): Promise<RootCauseIndexEntry[]> {
  return assembleRootCauseIndexFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadPatternIndex(projectId?: ProjectId): Promise<PatternIndexEntry[]> {
  return assemblePatternIndexFromAssembly(await loadAssemblyContextForProject(projectId));
}

export async function loadCaseDetail(id: string, projectId?: ProjectId): Promise<CaseDetail> {
  return assembleCaseDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadFunctionDetail(id: string, projectId?: ProjectId): Promise<FunctionDetail> {
  return assembleFunctionDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadPatternDetail(id: string, projectId?: ProjectId): Promise<PatternDetail> {
  return assemblePatternDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadRootCauseDetail(id: string, projectId?: ProjectId): Promise<RootCauseDetail> {
  return assembleRootCauseDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadComponentDetail(id: string, projectId?: ProjectId): Promise<ComponentDetail> {
  return assembleComponentDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadCategoryDetail(id: string, projectId?: ProjectId): Promise<CategoryDetail> {
  return assembleCategoryDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}

export async function loadArtifactDetail(id: string, projectId?: ProjectId): Promise<ArtifactDetail> {
  return assembleArtifactDetailFromAssembly(await loadAssemblyContextForProject(projectId), id);
}
