import { Col, DatePicker, List, Row, Space } from "antd";
import queryHooks from "../RESTservice/queryHooks";
import Card from "./Card";
import moment from "moment";
import React, { useState } from "react";
import Avatar from "antd/lib/avatar/avatar";
import {
  MessageOutlined,
  LikeOutlined,
  RetweetOutlined,
} from "@ant-design/icons";

const TweetsTimeRange = () => {
  const [dateRange, setDateRange] = useState([]);
  const { data, isFetching, isFetched } = queryHooks.useGetTweetsTimeRange(
    dateRange,
    {
      placeholderData: [],
      enabled: dateRange.length !== 0,
      refetchInterval: false,
    }
  );

  const onChange = (date, dateString) => {
    setDateRange(
      dateString.map((date) => date.split(":")[0].replace(" ", "-"))
    );
  };
  const IconText = ({ icon, text }) => (
    <Space>
      {React.createElement(icon)}
      {text}
    </Space>
  );

  return (
    <>
      <Row justify="space-between" align="middle">
        <Space size={20} align="start">
          <h2 style={{ color: "#fff" }}>توییت‌ها</h2>
          <DatePicker.RangePicker
            showTime={{
              defaultValue: [
                moment("00:00:00", "HH:mm:ss"),
                moment("11:59:59", "HH:mm:ss"),
              ],
            }}
            format="YYYY-MM-DD HH:mm:ss"
            onChange={onChange}
          />
        </Space>
        {isFetched && <h3 style={{ color: "#fff" }}>{data.length} توییت</h3>}
      </Row>
      <Card>
        <Row>
          <Col span={24}>
            {dateRange.length !== 0 ? (
              <List
                itemLayout="horizontal"
                dataSource={data}
                loading={isFetching}
                size="large"
                renderItem={(item) => (
                  <List.Item
                    actions={[
                      item.created_at.substring(0, 19),
                      <IconText
                        icon={RetweetOutlined}
                        text={item.retweet_count + item.quote_count}
                        key="1"
                      />,
                      <IconText
                        icon={LikeOutlined}
                        text={item.favorite_count}
                        key="2"
                      />,
                      <IconText
                        icon={MessageOutlined}
                        text={item.reply_count}
                        key="3"
                      />,
                    ]}
                  >
                    <List.Item.Meta
                      avatar={
                        <Avatar
                          size="large"
                          src={item.user.profile_image_url}
                        />
                      }
                      title={`${item.user.name}`}
                      description={item.text}
                    />
                  </List.Item>
                )}
              />
            ) : (
              <h2>بازه زمانی را انتخاب کنید</h2>
            )}
          </Col>
        </Row>
      </Card>
    </>
  );
};
export default TweetsTimeRange;
