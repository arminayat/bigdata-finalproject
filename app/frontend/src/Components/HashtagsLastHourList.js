import { List, Row } from "antd";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";

const HashtagsLastHourList = () => {
  const { data, isFetching } = queryHooks.useGetLastHourHashtags();
  return (
    <>
      <Row justify="space-between" align="bottom">
        <h2 style={{ color: "#fff" }}>هشتگ‌های یکساعت‌ اخیر</h2>
        <p style={{ color: "#fff" }}>{`تعداد ${data?.length}`}</p>
      </Row>

      <Card>
        <List
          dataSource={data}
          loading={isFetching}
          renderItem={(item) => (
            <List.Item>
              <List.Item.Meta title={`#${item}`} />
            </List.Item>
          )}
        />
      </Card>
    </>
  );
};
export default HashtagsLastHourList;
