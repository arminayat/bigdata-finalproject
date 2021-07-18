import styled from "styled-components";

const Card = styled.div`
  width: 100%;
  height: 100%;
  background-color: #fff;
  border-radius: 2rem;
  box-shadow: 0 0 5rem 0 rgba(0, 0, 0, .02);
  padding: 2rem;
  overflow-x: scroll;
  overflow-y: scroll;
  overflow-x: hidden;
  ::-webkit-scrollbar {
    width: 5px; /* Remove scrollbar space */
    background: transparent; /* Optional: just make scrollbar invisible */
  }
  ::-webkit-scrollbar-thumb {
    background: #ccc;
    border-radius: 10rem;
  }
`;

export default Card;
