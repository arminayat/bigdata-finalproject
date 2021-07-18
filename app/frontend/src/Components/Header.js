import { Space } from "antd";
import { TwitterOutlined } from "@ant-design/icons";

const Header = () => {
  return (
    <Space direction="vertical">
      <TwitterOutlined style={{ color: "#fff", fontSize: "7rem" }} />
      <h1 style={{ color: "#fff", fontSize: "5rem" }}>داشبورد توییتر</h1>
    </Space>
  );
};
export default Header;
