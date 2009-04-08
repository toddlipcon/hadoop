<?php
/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
include_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';

include_once $GLOBALS['THRIFT_ROOT'].'/packages/hdfs/hdfs_types.php';

interface DatanodeIf {
  public function readBlock($block, $offset, $length);
}

class DatanodeClient implements DatanodeIf {
  protected $input_ = null;
  protected $output_ = null;

  protected $seqid_ = 0;

  public function __construct($input, $output=null) {
    $this->input_ = $input;
    $this->output_ = $output ? $output : $input;
  }

  public function readBlock($block, $offset, $length)
  {
    $this->send_readBlock($block, $offset, $length);
    return $this->recv_readBlock();
  }

  public function send_readBlock($block, $offset, $length)
  {
    $args = new hadoop_api_Datanode_readBlock_args();
    $args->block = $block;
    $args->offset = $offset;
    $args->length = $length;
    $bin_accel = ($this->output_ instanceof TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'readBlock', TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('readBlock', TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_readBlock()
  {
    $bin_accel = ($this->input_ instanceof TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, 'hadoop_api_Datanode_readBlock_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == TMessageType::EXCEPTION) {
        $x = new TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new hadoop_api_Datanode_readBlock_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->err !== null) {
      throw $result->err;
    }
    throw new Exception("readBlock failed: unknown result");
  }

}

// HELPER FUNCTIONS AND STRUCTURES

class hadoop_api_Datanode_readBlock_args {
  static $_TSPEC;

  public $block = null;
  public $offset = null;
  public $length = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'block',
          'type' => TType::STRUCT,
          'class' => 'hadoop_api_Block',
          ),
        2 => array(
          'var' => 'offset',
          'type' => TType::I64,
          ),
        3 => array(
          'var' => 'length',
          'type' => TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      if (isset($vals['block'])) {
        $this->block = $vals['block'];
      }
      if (isset($vals['offset'])) {
        $this->offset = $vals['offset'];
      }
      if (isset($vals['length'])) {
        $this->length = $vals['length'];
      }
    }
  }

  public function getName() {
    return 'Datanode_readBlock_args';
  }

  public function read($input)
  {
    $xfer = 0;
    $fname = null;
    $ftype = 0;
    $fid = 0;
    $xfer += $input->readStructBegin($fname);
    while (true)
    {
      $xfer += $input->readFieldBegin($fname, $ftype, $fid);
      if ($ftype == TType::STOP) {
        break;
      }
      switch ($fid)
      {
        case 1:
          if ($ftype == TType::STRUCT) {
            $this->block = new hadoop_api_Block();
            $xfer += $this->block->read($input);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 2:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->offset);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 3:
          if ($ftype == TType::I32) {
            $xfer += $input->readI32($this->length);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        default:
          $xfer += $input->skip($ftype);
          break;
      }
      $xfer += $input->readFieldEnd();
    }
    $xfer += $input->readStructEnd();
    return $xfer;
  }

  public function write($output) {
    $xfer = 0;
    $xfer += $output->writeStructBegin('Datanode_readBlock_args');
    if ($this->block !== null) {
      if (!is_object($this->block)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('block', TType::STRUCT, 1);
      $xfer += $this->block->write($output);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->offset !== null) {
      $xfer += $output->writeFieldBegin('offset', TType::I64, 2);
      $xfer += $output->writeI64($this->offset);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->length !== null) {
      $xfer += $output->writeFieldBegin('length', TType::I32, 3);
      $xfer += $output->writeI32($this->length);
      $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
  }

}

class hadoop_api_Datanode_readBlock_result {
  static $_TSPEC;

  public $success = null;
  public $err = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        0 => array(
          'var' => 'success',
          'type' => TType::STRUCT,
          'class' => 'hadoop_api_BlockData',
          ),
        1 => array(
          'var' => 'err',
          'type' => TType::STRUCT,
          'class' => 'hadoop_api_IOException',
          ),
        );
    }
    if (is_array($vals)) {
      if (isset($vals['success'])) {
        $this->success = $vals['success'];
      }
      if (isset($vals['err'])) {
        $this->err = $vals['err'];
      }
    }
  }

  public function getName() {
    return 'Datanode_readBlock_result';
  }

  public function read($input)
  {
    $xfer = 0;
    $fname = null;
    $ftype = 0;
    $fid = 0;
    $xfer += $input->readStructBegin($fname);
    while (true)
    {
      $xfer += $input->readFieldBegin($fname, $ftype, $fid);
      if ($ftype == TType::STOP) {
        break;
      }
      switch ($fid)
      {
        case 0:
          if ($ftype == TType::STRUCT) {
            $this->success = new hadoop_api_BlockData();
            $xfer += $this->success->read($input);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 1:
          if ($ftype == TType::STRUCT) {
            $this->err = new hadoop_api_IOException();
            $xfer += $this->err->read($input);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        default:
          $xfer += $input->skip($ftype);
          break;
      }
      $xfer += $input->readFieldEnd();
    }
    $xfer += $input->readStructEnd();
    return $xfer;
  }

  public function write($output) {
    $xfer = 0;
    $xfer += $output->writeStructBegin('Datanode_readBlock_result');
    if ($this->success !== null) {
      if (!is_object($this->success)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('success', TType::STRUCT, 0);
      $xfer += $this->success->write($output);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->err !== null) {
      $xfer += $output->writeFieldBegin('err', TType::STRUCT, 1);
      $xfer += $this->err->write($output);
      $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
  }

}

?>
