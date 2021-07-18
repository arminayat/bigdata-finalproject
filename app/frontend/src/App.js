import React from "react";
import { BrowserRouter, Switch, Route, useHistory } from "react-router-dom";
import Layout from "./Styles/Layout";
// import Dashboard from "./Pages/Dashboard";

function App() {
  return (
    <BrowserRouter>
      <Layout style={{}}>
        <Layout.Content>
          <Switch>
            <Route path="/">{/* <Dashboard /> */}</Route>
          </Switch>
        </Layout.Content>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
