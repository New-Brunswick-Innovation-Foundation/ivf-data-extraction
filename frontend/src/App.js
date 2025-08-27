import React from "react";
import CityResolver from "./CityResolver";
import PersonResolver from "./PersonResolver";
import CompanyResolver from "./CompanyResolver";
import Summary from "./Summary";

function App() {
  return (
    <div>
      <h1>IVF Data Extraction UI</h1>
      <CityResolver />
      <hr />
      <PersonResolver />
      <hr />
      <CompanyResolver />
      <hr />
      <Summary />
    </div>
  );
}

export default App;
