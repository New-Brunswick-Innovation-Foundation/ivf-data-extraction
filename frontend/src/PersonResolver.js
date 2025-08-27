import React, { useState, useEffect } from "react";
import { getNextPersonDuplicate, resolvePersonDuplicate } from "./api";

export default function PersonResolver() {
  const [dup, setDup] = useState(null);

  useEffect(() => {
    getNextPersonDuplicate().then((res) => setDup(res.data));
  }, []);

  const handleChoice = (choice, targetId = null) => {
    resolvePersonDuplicate(choice, targetId).then(() =>
      getNextPersonDuplicate().then((res) => setDup(res.data))
    );
  };

  if (!dup) return <p>Loading...</p>;
  if (dup.done) return <p>✅ All person duplicates resolved</p>;

  return (
    <div>
      <h3>Duplicate found for: {dup.name}</h3>
      <p>Email: {dup.email}</p>
      <ul>
        {dup.matches.map((m) => (
          <li key={m.id}>
            {m.name} ({m.similarity}) – {m.email}
          </li>
        ))}
      </ul>
      {dup.options.map((opt, i) => (
        <button key={i} onClick={() => handleChoice(i + 1, dup.matches[0]?.id)}>
          {opt}
        </button>
      ))}
    </div>
  );
}
