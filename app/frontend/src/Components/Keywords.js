import { List } from "antd";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";

const Keywords = () => {
  const { data, isFetching } = queryHooks.useGetKeywords();
  return (
    <>
      <h2 style={{ color: "#fff" }}>تعداد کلمات کلیدی شش ساعت اخیر</h2>
      <Card>
        <List
          dataSource={Object.entries(data || {})}
          loading={isFetching}
          renderItem={([key, value]) => (
            <List.Item>
              <List.Item.Meta title={key} description={value} />
            </List.Item>
          )}
        />
      </Card>
    </>
  );
};
export default Keywords;
