import { Col, Row } from "antd";
import HashtagsLastHourList from "../Components/HashtagsLastHourList";
import HashtagsList from "../Components/HashtagsList";
import Header from "../Components/Header";
import UpdatedTimer from "../Components/Timer";
import TweetsList from "../Components/TweetsList";
import TweetsTimeRange from "../Components/TweetsTimeRange";

const Dashboard = () => {
  return (
    <div style={{ padding: "10rem" }}>
      <Row gutter={[30, 80]}>
        <Col span={24}>
          <Row justify="space-between" style={{ color: "#fff" }} align="top">
            <Header />
            <UpdatedTimer />
          </Row>
        </Col>

        <Col span={12} style={{ height: "40rem" }}>
          <TweetsList />
        </Col>
        <Col span={6} style={{ height: "40rem" }}>
          <HashtagsList />
        </Col>
        <Col span={6} style={{ height: "40rem" }}>
          <HashtagsLastHourList />
        </Col>
        <Col span={24} style={{ height: "60rem" }}>
          <TweetsTimeRange />
        </Col>
      </Row>
    </div>
  );
};
export default Dashboard;
