import axios from "axios";

export const BASE_URL =
  "http://162.55.30.239:5000/";
export const API_URL = BASE_URL + "";

const API = axios.create({
  baseURL: API_URL,
  timeout: 10000,
});

export default API;
