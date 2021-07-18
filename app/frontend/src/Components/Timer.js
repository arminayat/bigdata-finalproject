import { useEffect, useState } from "react";

const UpdatedTimer = () => {
  const [timer, setTimer] = useState(10);
  useEffect(() => {
    let Interval = setInterval(
      () => setTimer(timer > 0 ? timer - 1 : 10),
      1000
    );
    return () => {
      clearInterval(Interval);
    };
  });
  return `آخرین بروزرسانی در ${timer} ثانیه قبل`;
};
export default UpdatedTimer;
