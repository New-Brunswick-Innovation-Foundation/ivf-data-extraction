import axios from "axios";

const API_BASE = "http://localhost:8000";

export const getNextCity = () => axios.get(`${API_BASE}/next-city`);
export const assignRegion = (city, region) =>
  axios.post(`${API_BASE}/assign-region`, { city, region });

export const getNextPersonDuplicate = () =>
  axios.get(`${API_BASE}/next-person-duplicate`);
export const resolvePersonDuplicate = (choice, targetId = null) =>
  axios.post(`${API_BASE}/resolve-person-duplicate`, {
    choice,
    target_id: targetId,
  });

export const getNextCompanyDuplicate = () =>
  axios.get(`${API_BASE}/next-company-duplicate`);
export const resolveCompanyDuplicate = (choice, targetId = null) =>
  axios.post(`${API_BASE}/resolve-company-duplicate`, {
    choice,
    target_id: targetId,
  });

export const getSummary = () => axios.get(`${API_BASE}/summary`);
