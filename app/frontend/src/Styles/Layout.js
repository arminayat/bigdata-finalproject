import { Layout } from "antd";
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
  height: 100%;
  min-height: 100vh;
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
export default StyledLayout;
