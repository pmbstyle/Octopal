import { isRouteErrorResponse, Link, useRouteError } from "react-router";

export function RouteErrorBoundary() {
  const error = useRouteError();

  const message = isRouteErrorResponse(error)
    ? `${error.status} ${error.statusText}`
    : error instanceof Error
      ? error.message
      : "Unknown routing error";

  return (
    <section className="panel panel-error">
      <h2>Something went wrong</h2>
      <p>{message}</p>
      <Link className="retry-link" to="/overview">
        Go back to Overview
      </Link>
    </section>
  );
}
