
public class MathRandom {
    /** 
     * 0���ֵĸ���Ϊ%50 
     */  
 public static double rate0 = 0.35;  
 /** 
     * 1���ֵĸ���Ϊ%20 
     */  
 public static double rate1 = 0.25;  
 /** 
     * 2���ֵĸ���Ϊ%15 
     */  
 public static double rate2 = 0.20;  
 /** 
     * 3���ֵĸ���Ϊ%10 
     */  
 public static double rate3 = 0.10;  
 /** 
     * 4���ֵĸ���Ϊ%4 
     */  
 public static double rate4 = 0.10;  
 /** 
     * 5���ֵĸ���Ϊ%1 
     */  
 public static double rate5 = 0.00;  
  
 /** 
  * Math.random()����һ��double�͵���������ж�һ�� 
  * ����0���ֵĸ���Ϊ%50�������0��0.50�м�ķ���0 
     * @return int 
     * 
     */  
 public int PercentageRandom()  
 {  
  double randomNumber;  
  randomNumber = Math.random();  
  if (randomNumber >= 0 && randomNumber <= rate0)  
  {  
   return 0;  
  }  
  else if (randomNumber >= rate0 && randomNumber <= rate0 + rate1)  
  {  
   return 1;  
  }  
  else if (randomNumber >= rate0 + rate1  
    && randomNumber <= rate0 + rate1 + rate2)  
  {  
   return 2;  
  }  
  else if (randomNumber >= rate0 + rate1 + rate2  
    && randomNumber <= rate0 + rate1 + rate2 + rate3)  
  {  
   return 3;  
  }  
  else if (randomNumber >= rate0 + rate1 + rate2 + rate3  
    && randomNumber <= rate0 + rate1 + rate2 + rate3 + rate4)  
  {  
   return 4;  
  }  
  else if (randomNumber >= rate0 + rate1 + rate2 + rate3 + rate4  
    && randomNumber <= rate0 + rate1 + rate2 + rate3 + rate4  
      + rate5)  
  {  
   return 5;  
  }  
  return -1;  
 }  
}
