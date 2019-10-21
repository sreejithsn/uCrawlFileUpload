package hello;

import com.isl.upstart.ucrawlex.common.dtos.VendorDTO;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateResponseDTO {
  private String id;
  private String title;
  private VendorDTO vendor;
  private String status;
  private Long createdAt;
  private String createdBy;
  private Integer candidateSize;
}
