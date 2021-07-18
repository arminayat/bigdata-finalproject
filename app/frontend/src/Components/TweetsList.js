import { List } from "antd";
import styled from "styled-components";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";

const TweetsList = () => {
  const { data, isFetching } = queryHooks.useGetLastTweets();
  return (
    <>
      <h2 style={{ color: "#fff" }}> ۱۰۰ توییت‌ اخیر</h2>
      <Card>
        <List
          itemLayout="horizontal"
          dataSource={data}
          loading={isFetching}
          renderItem={(item) => (
            <List.Item>
              <List.Item.Meta
                title={`${item.user.name}`}
                description={item.text}
              />
            </List.Item>
          )}
        />
      </Card>
    </>
  );
};
export default TweetsList;
