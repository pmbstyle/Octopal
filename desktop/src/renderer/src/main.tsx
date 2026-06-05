import React from "react";
import ReactDOM from "react-dom/client";

import { App } from "./App";
import { DesignPreview } from "./components/DesignPreview";
import "./styles.css";

const Root = import.meta.env.DEV && new URLSearchParams(window.location.search).get("preview") === "desk"
  ? DesignPreview
  : App;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>,
);
