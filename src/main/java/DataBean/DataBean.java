package DataBean;

import java.util.Date;

/**
 * @ClassName DataBean
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class DataBean {

    private String intentId = "";
    private String deptCode = "";
    private int fundcode = 0;
    private Date loandate = null;
    private int nstate = 0;
    private int userid = 0;
    private int lamount = 0 ;

    public String getIntentId() {
        return intentId;
    }

    public void setIntentId(String intentId) {
        this.intentId = intentId;
    }

    public String getDeptCode() {
        return deptCode;
    }

    public void setDeptCode(String deptCode) {
        this.deptCode = deptCode;
    }

    public int getFundcode() {
        return fundcode;
    }

    public void setFundcode(int fundcode) {
        this.fundcode = fundcode;
    }

    public Date getLoandate() {
        return loandate;
    }

    public void setLoandate(Date loandate) {
        this.loandate = loandate;
    }

    public int getNstate() {
        return nstate;
    }

    public void setNstate(int nstate) {
        this.nstate = nstate;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public int getLamount() {
        return lamount;
    }

    public void setLamount(int lamount) {
        this.lamount = lamount;
    }
}
