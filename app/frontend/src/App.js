import React from "react";
import { BrowserRouter, Switch, Route, useHistory } from "react-router-dom";
import { Layout } from "antd";
import { Content } from "antd/lib/layout/layout";
import Dashboard from "./Pages/Dashboard";
import styled, { keyframes } from "styled-components";

const Gradient = keyframes`
  0% {
    background-position: 0% 50%
    }
  50% {
  background-position: 100% 50%
  }
  100% {
  background-position: 0% 50%
  }`;
const StyledLayout = styled(Layout)`
  background: -webkit-linear-gradient(
    -45deg,
    #0052b6,
    #0080ce,
    #1da1f2,
    #00bef6,
    #1da1f2,
    #006eb9,
    #6f94bc,
    #0052b6
  );
  background: linear-gradient(
    -45deg,
    #0052b6,
    #0080ce,
    #1da1f2,
    #00bef6,
    #1da1f2,
    #006eb9,
    #6f94bc,
    #0052b6
  );
  background-size: 400% 400%;
  -webkit-animation: ${Gradient} 15s ease infinite;
  -moz-animation: ${Gradient} 15s ease infinite;
  animation: ${Gradient} 15s ease infinite;
`;

function App() {
  return (
    <BrowserRouter>
      <StyledLayout
        style={{
          height: "100%",
          minHeight: "100vh",
        }}
      >
        <Content>
          <Switch>
            <Route path="/">
              {/* <Dashboard /> */}
            </Route>
          </Switch>
        </Content>
      </StyledLayout>
    </BrowserRouter>
  );
}

export default App;
