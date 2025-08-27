import React, { useState, useEffect } from "react";
import { getSummary } from "./api";

export default function Summary() {
  const [summary, setSummary] = useState(null);

  useEffect(() => {
    getSummary().then((res) => setSummary(res.data));
  }, []);

  if (!summary) return <p>Loading...</p>;

  return (
    <div>
      <h3>Summary</h3>
      <pre>{JSON.stringify(summary, null, 2)}</pre>
    </div>
  );
}
