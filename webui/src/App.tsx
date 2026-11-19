import { Title } from "@solidjs/meta";
import { Loading } from "solid-js";
import { Router } from "./router";
import "./App.css";

export default function App() {
  return (
    <Router>
      {(props) => (
        <>
          <Title>Ochi logs viewer</Title>
          <Loading fallback={<main class="px-4 py-12">Loading…</main>}>{props.children}</Loading>
        </>
      )}
    </Router>
  );
}
