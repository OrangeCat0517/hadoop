package com.example.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmpDept implements Writable {
    private String empNo = "";
    private String empName = "";
    private String deptNo = "";
    private String deptName = "";
    private int flag; //1表示这是一个部门，0是一个员工

    public EmpDept() {
    }

    public EmpDept(EmpDept ed) {
        this.empNo = ed.getEmpNo();
        this.empName = ed.getEmpName();
        this.deptNo = ed.getDeptNo();
        this.deptName = ed.getDeptName();
    }

    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return String.format("%-15s%-15s%-15s%-15s\n", empNo, empName, deptNo, deptName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(empNo);
        out.writeUTF(empName);
        out.writeUTF(deptNo);
        out.writeUTF(deptName);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empNo = in.readUTF();
        this.empName = in.readUTF();
        this.deptNo = in.readUTF();
        this.deptName = in.readUTF();
        this.flag = in.readInt();
    }
}
