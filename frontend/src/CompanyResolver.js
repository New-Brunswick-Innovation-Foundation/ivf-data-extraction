import React, { useState, useEffect } from "react";
import { getNextCompanyDuplicate, resolveCompanyDuplicate } from "./api";

export default function CompanyResolver() {
  const [dup, setDup] = useState(null);

  useEffect(() => {
    getNextCompanyDuplicate().then((res) => setDup(res.data));
  }, []);

  const handleChoice = (choice, targetId = null) => {
    resolveCompanyDuplicate(choice, targetId).then(() =>
      getNextCompanyDuplicate().then((res) => setDup(res.data))
    );
  };

  if (!dup) return <p>Loading...</p>;
  if (dup.done) return <p>✅ All company duplicates resolved</p>;

  return (
    <div>
      <h3>Duplicate found for: {dup.name}</h3>
      <p>Address: {dup.address}</p>
      <ul>
        {dup.matches.map((m) => (
          <li key={m.id}>
            {m.name} ({m.similarity}) – {m.address}
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
