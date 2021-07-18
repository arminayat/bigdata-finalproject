import API from "./API";
import { useQuery, UseQueryOptions } from "react-query";

const fetchLastTweets = async () => {
  const { data } = await API.get("/last-tweets");
  console.log("fetchLastTweets", { data });
  return data;
};
const fetchLastHashtags = async () => {
  const { data } = await API.get("/last-hashtags");
  console.log("fetchLastHashtags", { data });
  return data;
};
const fetchLastHourHashtags = async () => {
    const { data } = await API.get("/hashtags");
    console.log("fetchLastHourHashtags", { data });
    return data;
  };
const fetchTweetsTimeRange = async (dateRange) => {
  const { data } = await API.get("/time-filter", {
    params: { start: dateRange[0], end: dateRange[1] },
  });
  console.log("fetchTweetsTimeRange", { data });
  return data;
};

const queryHooks = {
  useGetLastTweets: (options) => {
    return useQuery("lastTweets", fetchLastTweets, options);
  },
  useGetLastHashtags: (options) => {
    return useQuery("lastHashtags", fetchLastHashtags, options);
  },
  useGetLastHourHashtags: (options) => {
    return useQuery("lastHourHashtags", fetchLastHourHashtags, options);
  },
  useGetTweetsTimeRange: (dateRange, options: UseQueryOptions) => {
    return useQuery(
      ["tweetsTimeRange", ...dateRange],
      () => fetchTweetsTimeRange(dateRange),
      options
    );
  },
};

export default queryHooks;
