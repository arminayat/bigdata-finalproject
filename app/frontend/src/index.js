import React from "react";
import ReactDOM from "react-dom";
import "antd/dist/antd.css";
import "./Styles/index.css";
import "./Styles/vazirFont.css";
import App from "./App";
import { QueryClientProvider } from "react-query";
import queryClient from "./RESTservice/queryClient";
import { ConfigProvider } from "antd";
import { createGlobalStyle } from "styled-components";

const GlobalStyle = createGlobalStyle`
:root {
  font-size: 10px;
}
`;

ReactDOM.render(
  <React.StrictMode>
    <GlobalStyle />
    <ConfigProvider direction="rtl">
      <QueryClientProvider client={queryClient}>
        <App />{" "}
      </QueryClientProvider>
    </ConfigProvider>
  </React.StrictMode>,
  document.getElementById("root")
);
