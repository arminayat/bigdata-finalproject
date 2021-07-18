import { List } from "antd";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";

const HashtagsLastHourList = () => {
  const { data, isFetching } = queryHooks.useGetLastHourHashtags();
  return (
    <>
      <h2 style={{ color: "#fff" }}>هشتگ‌های یکساعت‌ اخیر</h2>
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
