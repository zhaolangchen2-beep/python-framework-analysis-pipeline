import {
  BrowserRouter,
  MemoryRouter,
  Navigate,
  Route,
  Routes,
} from "react-router-dom";
import { ProjectProvider } from "./context/ProjectContext";
import { AppShell } from "./layout/AppShell";
import { appRoutes } from "./routes";

type AppProps = {
  useMemoryRouter?: boolean;
  initialEntries?: string[];
};

function AppRoutes() {
  return (
    <AppShell>
      <Routes>
        {appRoutes.map((route) => (
          <Route key={route.path} path={route.path} element={route.element} />
        ))}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}

export default function App({ useMemoryRouter, initialEntries }: AppProps) {
  const router = useMemoryRouter ? (
    <MemoryRouter initialEntries={initialEntries}>
      <AppRoutes />
    </MemoryRouter>
  ) : (
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  );

  return <ProjectProvider>{router}</ProjectProvider>;
}
