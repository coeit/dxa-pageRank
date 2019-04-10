package de.hhu.bsinfo.dxapp.tasks;

import java.util.ArrayList;

public class ReadVertex {

    public int m_outdeg;
    public ArrayList<Integer> m_inEdges = new ArrayList<>();

    public ReadVertex(){
    }

    public void setOutdeg(int p_outDeg){
        m_outdeg = p_outDeg;
    }

}
