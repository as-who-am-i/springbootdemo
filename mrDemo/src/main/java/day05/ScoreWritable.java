package day05;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Prigram: day05
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-15 11:12
 */
public class ScoreWritable implements WritableComparable<ScoreWritable> {

    int id;
    int chinese;
    int math;
    int english;
    int sum;

    public ScoreWritable() {

    }

    public ScoreWritable(int id, int chinese, int math, int english) {
        this.id = id;
        this.chinese = chinese;
        this.math = math;
        this.english = english;
        this.sum = chinese + math + english;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getChinese() {
        return chinese;
    }

    public void setChinese(int chinese) {
        this.chinese = chinese;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "ScoreWritable{" +
                "id=" + id +
                ",chinese=" + chinese +
                ", math=" + math +
                ", english=" + english +
                ", sum=" + sum +
                '}';
    }

    //比较
    public int compareTo(ScoreWritable studentScore) {
        //在比较成绩的过程中线比较总成绩
        if (this.sum > studentScore.sum) {
            return -1;
        } else if (this.sum < studentScore.sum) {
            return 1;
        } else {
            if (this.chinese > studentScore.chinese) {
                return -1;
            } else if (this.chinese < studentScore.chinese) {
                return 1;
            } else {
                return studentScore.math - this.math;
            }
        }
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(chinese);
        dataOutput.writeInt(math);
        dataOutput.writeInt(english);
        dataOutput.writeInt(sum);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.chinese = dataInput.readInt();
        this.math = dataInput.readInt();
        this.english = dataInput.readInt();
        this.sum = dataInput.readInt();
    }
}
