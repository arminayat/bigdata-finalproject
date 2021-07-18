const { QueryClient } = require("react-query");

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { refetchOnWindowFocus: false, refetchInterval: 10 * 1000 },
  },
});
export default queryClient;
