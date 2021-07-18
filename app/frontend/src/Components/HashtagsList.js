import { List } from "antd";
import styled from "styled-components";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";

const HashtagsList = () => {
  const { data, isFetching } = queryHooks.useGetLastHashtags();
  return (
    <>
      <h2 style={{ color: "#fff" }}>۱۰۰۰ هشتگ‌ اخیر</h2>
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
export default HashtagsList;
