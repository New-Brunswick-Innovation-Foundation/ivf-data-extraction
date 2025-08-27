import React, { useState, useEffect } from "react";
import { getNextCity, assignRegion } from "./api";

export default function CityResolver() {
  const [cityData, setCityData] = useState(null);

  useEffect(() => {
    getNextCity().then((res) => setCityData(res.data));
  }, []);

  const handleRegion = (region) => {
    assignRegion(cityData.city, region).then(() => {
      getNextCity().then((res) => setCityData(res.data));
    });
  };

  if (!cityData) return <p>Loading...</p>;
  if (cityData.done) return <p>✅ All cities resolved</p>;

  return (
    <div>
      <h3>Assign region for: {cityData.city}</h3>
      {cityData.options.map((opt) => (
        <button key={opt} onClick={() => handleRegion(opt)}>
          {opt}
        </button>
      ))}
    </div>
  );
}
